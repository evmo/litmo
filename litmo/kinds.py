"""The three artifact kinds — archive, mirror, fetch.

Each exposes the same three verbs against one artifact:

    status(ctx, art) -> Report      compare local and remote, move nothing
    pull(ctx, art, ...) -> None     remote -> here
    push(ctx, art, ...) -> bool     here -> remote; True if it changed anything

`ctx` carries the config, the transport and the loaded manifest, so the verbs
below read as what they do rather than as plumbing.

Both pulls stage. Nothing arriving from the bucket touches the working tree
until it has been downloaded in full, checked against the manifest's sizes and
digests, and — for an archive — hashed as a whole tree. Failures before install
leave the previous local copy exactly as it was. Archive installation swaps a
whole tree; mirror installation moves independently named files one at a time,
so an unexpected operating-system failure in that final step can leave a
partially updated mirror.
"""

from __future__ import annotations

import concurrent.futures as cf
import contextlib
import errno
import hashlib
import json
import mimetypes
import os
import shutil
import stat
import tarfile
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

try:
    import fcntl
except ImportError:                     # a platform without POSIX locks
    fcntl = None

from . import paths
from .config import LEGACY_STATE_DIR, STATE_DIR, state_dir
from .hashing import file_sha256, human, stamps, tree_hash
from .remote import Oversized, fetch_url, head

ZSTD_LEVEL = 10
# `.litkit` is here as well as `.litmo` because a checkout that has not
# run since the rename still has one, and a leftover state directory must
# never be walked into an artifact.
SKIP_DIRS = {"__pycache__", ".ipynb_checkpoints", ".git",
             STATE_DIR, LEGACY_STATE_DIR}

# Belt and braces for the unpack: the bundle's digest is checked first, so
# reaching either of these means the bucket and the manifest already disagree.
MAX_MEMBERS = 2_000_000
MAX_UNPACKED = 256 << 30

# The ceiling on one downloaded object, applied *before* the transfer starts.
# Every other bound on a download is the size the manifest promises, and the
# manifest is the document being distrusted: `Manifest` accepts any
# non-negative integer, so a corrupt or hostile bucket that says 10**30 has
# removed the only limit on its own body and streams until the staging
# filesystem fills — measured, 11.9 GB into staging in three seconds against
# a server that simply never stops. An untrusted number may tighten this
# bound; it may not remove it. Deliberately far above anything real, like
# `remote.MAX_FETCH`: the largest archive any consumer publishes today is 65 MB
# and the largest single mirrored file is 97 MB, so this leaves three orders
# of magnitude, and it stays well under `MAX_UNPACKED` above.
MAX_OBJECT = 64 << 30

# How old a leftover staging tree must be before a later run sweeps it away.
STALE_STAGE = 24 * 3600

# How long an install waits for another litmo process to finish installing the
# same artifact. Installation is a rename, or for a merge a per-file move loop
# — 1.2 s at 50,000 files — so a wait this long means the holder is wedged or
# stopped rather than slow, and refusing beats hanging a pull forever. The one
# genuinely slow case is `_move`'s cross-filesystem copy fallback, which is why
# the refusal names the artifact rather than blaming the other process.
INSTALL_WAIT = 300

IN_SYNC, DIFFERS, LOCAL_ONLY, REMOTE_ONLY, ABSENT = (
    "in sync", "DIFFERS", "local only", "remote only", "absent")


@dataclass
class Report:
    local: str
    remote: str
    verdict: str

    @property
    def ok(self) -> bool:
        return self.verdict in (IN_SYNC, ABSENT)


def _zstd():
    try:
        import zstandard
    except ImportError:
        raise SystemExit(
            "the `archive` kind needs zstandard — install litmo with the "
            "`archive` extra:\n"
            "  uv add 'litmo[archive] @ git+https://github.com/evmo/litmo'"
        ) from None
    return zstandard


# ----------------------------------------------------------- where things go -

def _here(ctx, art) -> Path:
    """The artifact's own directory. `config.load` proved it relative."""
    return ctx.cfg.root / art.path


def _dest(ctx, art, rel: str) -> Path:
    """Where one manifest entry is allowed to land.

    The manifest was already checked for shape when it loaded; this is the
    half that can only be done against the filesystem, at the moment of
    writing — that no parent has become a symlink out of the tree since.
    """
    try:
        return paths.under_artifact(ctx.cfg.root, art, rel,
                                    what=f"artifact {art.name!r}: manifest path")
    except paths.Unsafe as e:
        raise SystemExit(f"  {e}") from None


def _a(kind) -> str:
    return f"an {kind}" if str(kind)[:1] in "aeiou" else f"a {kind}"


def _entry(ctx, art) -> dict | None:
    """The manifest entry for this artifact, if this kind can read it.

    `sync.toml` says what an artifact is; the manifest says what the bucket
    last held. The two can disagree — someone edits `kind`, or a legacy flat
    list is migrated — and nothing compared them, so every verb reached into
    an entry of the wrong shape and raised: KeyError on `tree_hash`, a
    TypeError formatting a list as a number, another iterating a file count.

    Refused by name here, for the verbs that have nothing to compare. `push`
    goes through `_prior` instead: replacing the entry is how the bucket is
    put right, and refusing that would leave a repository no way back.
    """
    e = ctx.manifest.get(art.name)
    if e and e.get("kind") != art.kind:
        mine, theirs = _a(art.kind), _a(e.get("kind"))
        raise SystemExit(
            f"  {art.name}: sync.toml declares this {mine}, but the manifest "
            f"in the bucket describes {theirs} — there is nothing here to "
            f"compare it with.\n"
            f"    `litmo push {art.name}` republishes it as {mine}.")
    return e


def _prior(ctx, art) -> dict:
    """The manifest entry a push should build on, or {} if there is none it
    can use. An entry of another kind is dropped with a note, not refused."""
    e = ctx.manifest.get(art.name) or {}
    if e and e.get("kind") != art.kind:
        print(f"  {art.name:9} replacing the manifest's {e.get('kind')} entry "
              f"— sync.toml declares this {_a(art.kind)}")
        return {}
    return e


def _staging(cfg):
    """A scratch directory on the same filesystem as the checkout.

    Verified content is moved into place with os.replace, which will not
    cross a filesystem boundary, so /tmp is not an option: the repository may
    well be on a different mount from it.

    A pull that is killed outright, rather than raising, leaves its staging
    tree behind — and that tree can be the size of the artifact. Anything a
    day old belongs to no live run, so it goes.
    """
    base = state_dir(cfg.root) / "tmp"
    base.mkdir(parents=True, exist_ok=True)
    cutoff = time.time() - STALE_STAGE
    for old in base.glob("stage-*"):
        with contextlib.suppress(OSError):
            if old.stat().st_mtime < cutoff:
                shutil.rmtree(old, ignore_errors=True)
    return tempfile.TemporaryDirectory(prefix="stage-", dir=base)


def _move(src: Path, dest: Path) -> None:
    """Staged -> installed. A rename where it can be, a copy where it cannot.

    An artifact directory that is a symlink onto another disk puts the staging
    area and the destination on different filesystems, where rename is not
    allowed. The bytes have already been verified by then, so falling back to
    a copy costs the per-file atomicity and nothing else.

    Not a bare `shutil.move`, though, which is not `os.replace` with a longer
    reach: it *follows* a symlink at `dest`, moving the file into the
    directory the link names or copying through it onto the file it names,
    where `os.replace` replaces the link itself. `_blocked` waves a link at
    `dest` through on exactly that promise, so on a cross-filesystem artifact
    a merge wrote outside it — reproduced both ways, a published file
    overwriting the target of a local link and another landing inside the
    directory one pointed at, with the link intact and the pull reporting
    success. Land beside `dest` on the destination's own filesystem instead,
    then rename over it, so the fallback replaces what the rename would have.
    """
    try:
        os.replace(src, dest)
    except OSError as e:
        if e.errno != errno.EXDEV:
            raise
        near = dest.parent / f".{dest.name}.litmo-part"
        if near.exists() or near.is_symlink():
            _discard(near)              # a run that was killed left it
        try:
            shutil.move(str(src), str(near))
        except BaseException:
            if near.exists() or near.is_symlink():
                _discard(near)
            raise
        os.replace(near, dest)


class Blocked(Exception):
    """Installing would fail partway, because the local layout is in the way.

    Raised before anything has moved, so the caller can refuse while the
    working tree is still exactly as it was.
    """

    def __init__(self, conflicts: list[tuple[Path, str]],
                 why: str = "installing would fail partway"):
        super().__init__(f"{len(conflicts)} path(s) in the way")
        self.conflicts = conflicts
        self.why = why


def _blocked(dest: Path, stop: Path) -> tuple[Path, str] | None:
    """Why a verified file could not be installed at `dest`, or None.

    `os.replace` of a file onto a directory raises IsADirectoryError, and
    `mkdir(parents=True)` of a parent that is already a file raises
    FileExistsError. Both happen inside the install step, past the point where
    a pull has promised the working tree is safe to change, and both leave it
    half-installed — so the layout is looked at before that point rather than
    after. It happens whenever a published path changes between a file and a
    directory and a reader still holds the old shape.

    A symlink *at* `dest` is not in the way: `os.replace` replaces the link
    itself. A symlink on the way *to* it is another matter — `mkdir(parents=
    True, exist_ok=True)` is content with a link to a directory and the move
    then writes straight through it, so a bundle naming `sub/x` installs into
    whatever `sub` points at. That is how a merge reaches outside the
    artifact without a `..` anywhere: the paths under an artifact root are
    joined, never resolved, because the tree they name was verified as a
    whole. `stop` itself is exempt — `_install` has already resolved it, and
    an artifact directory pointed at a scratch disk is a supported thing to
    have done.
    """
    if dest.is_dir() and not dest.is_symlink():
        return (dest, "a directory here, and a file in the bucket")
    # Up to and including `stop`: the artifact's own directory can be the
    # thing in the way, when the bucket holds a tree where this checkout
    # still holds a single file of that name.
    for anc in dest.parents:
        if anc != stop and stop not in anc.parents:
            break
        if anc.is_file():
            return (anc, "a file here, and a directory in the bucket")
        if anc != stop and anc.is_symlink():
            return (anc, "a symlink here, and a directory in the bucket — "
                         "the bucket's files would land wherever it points")
        if anc == stop:
            break
    return None


def _layout_lines(conflicts: list[tuple[Path, str]], root: Path) -> list[str]:
    """One line per distinct path in the way — several files under the same
    conflicting ancestor are one problem, not many."""
    seen: dict[str, str] = {}
    for p, why in conflicts:
        try:
            shown = p.relative_to(root).as_posix()
        except ValueError:
            shown = str(p)
        seen.setdefault(shown, why)
    # These are staged-tree and local names, not manifest ones — nothing
    # refused a control character in them on the way in. `paths.relative`
    # guards everything the bucket *names*; a bundle's member names it never
    # sees, and a refusal is the message most worth forging.
    return [f"    {paths.display(k)} is {v}" for k, v in seen.items()]


def _refuse_layout(art, conflicts: list[tuple[Path, str]], root: Path,
                   why: str, hint: str):
    lines = _layout_lines(conflicts, root)
    return SystemExit(
        f"  {art.name}: the published layout no longer fits what is here, so "
        f"{why} — {art.path} was not touched:\n"
        + "\n".join(lines[:10])
        + (f"\n    … and {len(lines) - 10:,} more" if len(lines) > 10 else "")
        + f"\n    {hint}")


def _discard(p: Path) -> None:
    """Remove one path, whatever shape it turned out to be."""
    if p.is_dir() and not p.is_symlink():
        shutil.rmtree(p)
    else:
        p.unlink()


@contextlib.contextmanager
def _installing(cfg, art):
    """Serialise one artifact's installation against other litmo processes.

    `_swap` parks the outgoing tree beside itself under a name derived from
    the destination, and reads an existing park as a dead run's leftover.
    Two pulls of one artifact therefore had a window in which the second
    deleted the first's only copy of the live tree. Reproduced with two
    threads against the real `_swap`: the second installed its verified tree
    while the first was still parked, the first's rename then failed
    ENOTEMPTY, its rollback deleted the second's tree and could not restore
    the park it no longer had, and the artifact was left absent — with the
    second pull having reported success.

    An advisory `flock` gives the park the privacy its fixed name already
    assumes, which is what makes the "a run that was killed left it" sweep
    inside `_swap` correct rather than a race: under the lock, a park that is
    there belongs to nobody.

    Held around installation only, not around staging. Two runs may still
    download and verify the same bundle concurrently — installing the same
    verified tree twice costs a rename and changes nothing — and holding it
    across a multi-gigabyte download would serialise work that does not
    conflict.

    Bounded, because a lock nobody releases must not turn a pull into a hang.
    The lock file lives in the state directory and is keyed on the artifact's
    *path*: the name is a TOML table key and can be any string at all, while
    the path is what is being protected and `config._check_layout` has
    already proved no two artifacts share one.

    Where `fcntl` is missing the install runs unserialised, exactly as it did
    before this existed — a platform without POSIX locks is not a reason to
    refuse to pull.
    """
    if fcntl is None:
        yield
        return
    locks = state_dir(cfg.root) / "locks"
    locks.mkdir(parents=True, exist_ok=True)
    key = hashlib.sha256(art.path.as_posix().encode()).hexdigest()[:12]
    # Never unlinked: removing a lock file races with the next process
    # opening it, and the two would then hold different inodes and the same
    # lock. They are empty and there is one per artifact.
    fh = (locks / f"{art.path.name}-{key}.lock").open("a+")
    try:
        deadline = time.monotonic() + INSTALL_WAIT
        said = False
        while True:
            try:
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError:
                if time.monotonic() >= deadline:
                    raise SystemExit(
                        f"  {art.name}: another litmo has been installing "
                        f"{art.path} for over {INSTALL_WAIT}s — {art.path} "
                        f"was not touched.\n"
                        f"    Installing two copies at once can leave the "
                        f"artifact absent, so this one stopped instead.\n"
                        f"    Wait for it to finish, or stop it, then re-run "
                        f"`litmo pull {art.name}`.") from None
                if not said:
                    print(f"  {art.name:9} waiting for another litmo to "
                          f"finish installing {art.path} …", flush=True)
                    said = True
                time.sleep(0.05)
        try:
            yield
        finally:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
    finally:
        fh.close()


def _swap(staged: Path, dest: Path) -> None:
    """Replace the whole of `dest` with the verified tree.

    Call it under `_installing`: the park below is named after `dest` alone,
    so two unserialised swaps of one artifact destroy each other.

    The outgoing copy is parked *beside* itself, and that is the whole of the
    care taken here: a rename within one directory has no filesystem boundary
    to cross, so parking always succeeds and is always undoable by renaming
    it back. Parked into the staging directory it was neither. An artifact
    symlinked onto another disk made that rename EXDEV, and the branch that
    caught EXDEV deleted the outgoing tree outright *before* starting the
    fallible cross-filesystem copy; even on one filesystem, a second step
    that raised left the destination absent and the only copy of it inside a
    temporary directory that was about to be removed. Either way a disk-full,
    I/O or concurrent-layout failure during the swap destroyed the local
    copy, on the one command whose bucket may be its only other copy.
    """
    parked = dest.parent / f".{dest.name}.litmo-old"
    if parked.exists() or parked.is_symlink():
        _discard(parked)                    # a run that was killed left it
    outgoing = dest.exists() or dest.is_symlink()
    if outgoing:
        os.replace(dest, parked)
    try:
        _move(staged, dest)
    except BaseException:
        if outgoing:
            with contextlib.suppress(OSError):
                if dest.exists() or dest.is_symlink():
                    _discard(dest)          # whatever the failed move left
                os.replace(parked, dest)
        raise
    if outgoing and (parked.exists() or parked.is_symlink()):
        _discard(parked)


def _install(staged: Path, dest: Path, *, clean: bool) -> bool:
    """Move a verified staging tree onto the destination. True if it merged.

    A merge is the one outcome the caller cannot predict: the destination
    ends up holding whatever local files the bundle did not name as well as
    the ones it did. Every other path renames the verified tree into place,
    so the caller already knows what is there.

    A destination that is a symlink is followed, not replaced: someone who
    pointed `out/` at a scratch disk meant the artifact to live there, and a
    pull has no business quietly turning it back into an ordinary directory.

    Where the whole tree is being swapped, `_swap` keeps the old one until
    the new one is in place, so the destination is never briefly absent and a
    failure puts it back. That swap is only ever `--clean`'s to make: it
    throws away whatever was there, and a merge may not destroy what it did
    not download.
    """
    if dest.is_symlink():
        dest = Path(os.path.realpath(dest))
    dest.parent.mkdir(parents=True, exist_ok=True)
    if not clean and dest.exists() and dest.is_dir() != staged.is_dir():
        # The artifact itself changed shape. Swapping would throw the local
        # copy away — silently, and reporting success. It is the same
        # conflict the merge refuses per file, one level up, and it costs
        # the whole artifact.
        raise Blocked([(dest, "a directory here, and a file in the bucket"
                              if dest.is_dir() else
                              "a file here, and a directory in the bucket")],
                      "the whole artifact would be replaced")
    if clean or not dest.exists() or dest.is_dir() != staged.is_dir():
        _swap(staged, dest)
        return False
    if staged.is_file():
        _move(staged, dest)
        return False
    files = sorted(p for p in staged.rglob("*") if p.is_file())
    blocked = [c for src in files
               if (c := _blocked(dest / src.relative_to(staged), dest))]
    if blocked:
        raise Blocked(blocked)
    for src in files:
        target = dest / src.relative_to(staged)
        target.parent.mkdir(parents=True, exist_ok=True)
        _move(src, target)
    return True


# --------------------------------------------------------------- archive ----
# One tar.zst bundle per artifact. For a tree of thousands of small files
# (cached API responses, say) this is the difference between one request and
# thousands. Identity is the hash of the *tree*, never of the bundle — see
# hashing.tree_hash.

class Escaping(Exception):
    """A symlink out of the artifact, found in the bytes being packed."""

    def __init__(self, link: str, target: Path):
        super().__init__(link)
        self.link, self.target = link, target


class Special(Exception):
    """A fifo or a device node, found in the bytes being packed."""

    def __init__(self, rel: str):
        super().__init__(rel)
        self.rel = rel


def _packable(mode: int) -> bool:
    """Is a member of this kind one a reader can extract?

    `tarfile.data_filter` takes regular files, directories and links; a fifo
    or a device node it refuses with SpecialFileError, whatever the bundle
    says. A socket never gets that far — `tarfile.gettarinfo` returns None
    for one and `add` skips it with a debug line — so it is packable in the
    only sense that matters here: nothing goes in, and nothing has to come
    back out.
    """
    return bool(stat.S_ISREG(mode) or stat.S_ISDIR(mode)
                or stat.S_ISLNK(mode) or stat.S_ISSOCK(mode))


def _escapes(link: Path, root: Path) -> Path | None:
    """Where `link` points, if that is outside `root`. None if it stays in."""
    target = Path(os.path.realpath(link))
    return None if target == root or root in target.parents else target


class _Tar(tarfile.TarFile):
    """A `TarFile` that leaves owner *names* out of the member headers.

    `TarFile.gettarinfo` finishes every member with a `pwd.getpwuid` and a
    `grp.getgrgid` to fill `uname`/`gname`. On a host whose
    `/etc/nsswitch.conf` merges systemd's user database into `group` — the
    Arch default — `getgrgid` is a socket round trip taken *after* the local
    file has already answered: 300 µs a call here against about 45 µs of real
    work per file, so packing a 20,000-file tree spent 94 % of its time
    naming owners nobody ever reads back. `_unpack` extracts with the `data`
    filter, which drops ownership outright, and an artifact's identity is
    `tree_hash`, which never looked at it.

    So the header is filled from the `lstat` alone. Numeric `uid`/`gid` are
    still recorded; the names stay empty, which is exactly what the stdlib
    itself writes on a platform with no `pwd` and `grp` module — and it makes
    a bundle independent of the publisher's account names. The cases that
    need more than an `lstat` to describe — a hardlink, a device, a fifo —
    are rare enough in an artifact to hand back to the stdlib and pay for.

    One line of the original is deliberately not copied: the back-reference
    `gettarinfo` plants on each member. 3.12 writes `tarinfo.tarfile = self`
    and calls it "Not needed" in the comment; 3.13 renamed it to `_tarfile`,
    made `tarfile` a property that warns, and marked it for removal in 3.16.
    Nothing in `tarfile` ever reads it back, so setting it bought a
    `DeprecationWarning` per member and an `AttributeError` in 3.16.
    """

    # The artifact root, resolved, for the escaping-link check below. None
    # when the artifact is a single file, which has nothing under it.
    link_root: Path | None = None

    def gettarinfo(self, name=None, arcname=None, fileobj=None):
        if fileobj is not None or self.dereference:
            return super().gettarinfo(name, arcname, fileobj)
        st = os.lstat(name)
        # The arcname `add` hands down is already the repo-relative path, so
        # a refusal below can name the member the way the rest of litmo does.
        where = str(name if arcname is None else arcname)
        if stat.S_ISREG(st.st_mode):
            if st.st_nlink > 1:              # may be a hardlink to a member
                return super().gettarinfo(name, arcname)
            kind, linkname, size = tarfile.REGTYPE, "", st.st_size
        elif stat.S_ISDIR(st.st_mode):
            kind, linkname, size = tarfile.DIRTYPE, "", 0
        elif stat.S_ISLNK(st.st_mode):
            # The two checks below are the only ones made on the bytes that
            # actually go into the bundle. `archive_push` refuses both kinds
            # before it starts, but a build still running underneath the
            # publish can create one after that, and nothing later notices:
            # `tree_hash` reads *through* a link both times it runs, so the
            # digest it re-checks is unchanged while tar stores a reference
            # to a path only this machine has, and it does not count a
            # special file at all.
            if self.link_root is not None and (
                    target := _escapes(Path(name), self.link_root)):
                raise Escaping(where, target)
            kind, linkname, size = tarfile.SYMTYPE, os.readlink(name), 0
        elif not _packable(st.st_mode):
            raise Special(where)
        else:
            return super().gettarinfo(name, arcname)

        info = self.tarinfo()
        _, arc = os.path.splitdrive(name if arcname is None else arcname)
        info.name = arc.replace(os.sep, "/").lstrip("/")
        info.mode = st.st_mode
        info.uid, info.gid = st.st_uid, st.st_gid
        info.size = size
        info.mtime = st.st_mtime
        info.type = kind
        info.linkname = linkname
        return info


def _pack(root: Path, src: Path, dest: Path) -> None:
    # tar.add stores a symlink *as* a symlink and does not recurse into it, so
    # an artifact directory pointed at a scratch disk would otherwise publish
    # as one dangling link and nothing else. tree_hash reads through it; so
    # must this, or the two disagree about what was published.
    arcname = src.relative_to(root).as_posix()
    real = Path(os.path.realpath(src))
    cctx = _zstd().ZstdCompressor(level=ZSTD_LEVEL, threads=-1)
    with dest.open("wb") as raw, cctx.stream_writer(raw) as z:
        with _Tar.open(fileobj=z, mode="w|") as tar:
            tar.link_root = real if real.is_dir() else None
            tar.add(real, arcname=arcname)


def _listing(art, some: list, total: int) -> str:
    """The first few offending paths of a refusal, one to a line."""
    return "\n".join(
        f"    {(art.path / rel).as_posix()}" + (f" -> {t}" if t else "")
        for rel, t in ((x if isinstance(x, tuple) else (x, None))
                       for x in some)) + (
        f"\n    … and {total - len(some):,} more" if total > len(some) else "")


def _unpublishable(src: Path) -> tuple[list[tuple[Path, Path]], list[Path]]:
    """Everything under `src` that a bundle cannot carry, in one walk:
    (escaping symlinks, special files).

    Both are the same failure. `tar.add` stores a link *as* a link, while
    `tree_hash` reads *through* it — so a link out of the artifact is hashed
    as its target's bytes but packed as a reference to a path only this
    machine has. A fifo or a device node is not hashed at all, `tree_hash`
    counting regular files only, but `tarfile` packs it faithfully. Either
    way the push exits 0, `status` says in sync, and every reader's pull dies
    on the `data` extraction filter, which refuses a link that leaves the
    destination and refuses a special file outright. Nothing on the
    publisher's side says so, and — because neither one moves the tree hash —
    the *next* push says "up to date" and never repacks, so the bucket stays
    broken until someone thinks to pass --force. The publishing end is the
    only place that can see the difference, so it is where this is refused,
    before the hash that would otherwise return early.

    A link that stays inside the artifact is fine, and stays fine: it packs,
    extracts and hashes the same on both sides. So is a socket, which
    `tarfile` declines to pack at all.
    """
    root = Path(os.path.realpath(src))
    if not root.is_dir():
        return ([], [])                # the artifact is one file; _pack reads
                                       # through it and stores a regular file
    links, special = [], []
    for p in sorted(root.rglob("*")):  # rglob does not descend through links
        if p.is_symlink():
            if target := _escapes(p, root):
                links.append((p.relative_to(root), target))
        elif not _packable(p.lstat().st_mode):
            special.append(p.relative_to(root))
    return (links, special)


def _unpack(archive: Path, root: Path) -> None:
    """Extract into `root`, one member at a time so the counters can bite.

    `filter="data"` is what refuses absolute names, `..`, devices and links
    out of the tree; the counters are the cheap guard against a bundle whose
    digest matched but whose contents are absurd.
    """
    dctx = _zstd().ZstdDecompressor()
    members = unpacked = 0
    with archive.open("rb") as raw, dctx.stream_reader(raw) as z:
        with tarfile.open(fileobj=z, mode="r|") as tar:
            for member in tar:
                members += 1
                unpacked += max(member.size, 0)
                if members > MAX_MEMBERS:
                    raise SystemExit(f"  archive holds more than "
                                     f"{MAX_MEMBERS:,} members — refusing")
                if unpacked > MAX_UNPACKED:
                    raise SystemExit(f"  archive unpacks to more than "
                                     f"{human(MAX_UNPACKED)} — refusing")
                tar.extract(member, root, filter="data")


def archive_status(ctx, art) -> Report:
    digest, n, size = tree_hash(_here(ctx, art))
    remote = _entry(ctx, art) or {}
    local_s = f"{n:,} files, {human(size)}" if digest else "absent"
    remote_s = (f"{remote.get('files', 0):,} files, "
                f"{human(remote.get('raw_bytes', 0))}" if remote else "absent")
    if not remote and not digest:
        verdict = ABSENT
    elif not remote:
        verdict = LOCAL_ONLY
    elif not digest:
        verdict = REMOTE_ONLY
    else:
        verdict = IN_SYNC if digest == remote.get("tree_hash") else DIFFERS
    return Report(local_s, remote_s, verdict)


def archive_pull(ctx, art, *, force=False, clean=False) -> None:
    remote = _entry(ctx, art)
    if not remote:
        print(f"  {art.name:9} not in the bucket")
        return
    dest = _here(ctx, art)
    local_hash, n, size = tree_hash(dest)
    if local_hash == remote["tree_hash"] and not force:
        print(f"  {art.name:9} up to date  ({n:,} files, {human(size)})")
        return

    # The only bound on this download is the size the manifest promises, and
    # `manifest._int` lets that be absent or zero — at which point `x or None`
    # handed `download` "no limit" and a lying bucket could write until the
    # disk filled, the sha-256 being checked only once the whole body is down.
    # An untrusted number may tighten this bound; it may not remove it. Zero
    # is not a size a bundle can have (`archive_push` writes the packed file's
    # own length), so refuse by name rather than invent a ceiling, and say
    # which command mends the bucket. Refused here and not in `Manifest.load`
    # so that the `push` which would rewrite the entry can still read it.
    promised = remote.get("archive_bytes") or 0
    if promised <= 0:
        raise SystemExit(
            f"  {art.name}: the manifest in the bucket does not say how big "
            f"{remote['key']} is, so there is no size to hold the download "
            f"to — {art.path} was not touched.\n"
            f"    `litmo push {art.name}` republishes it with one.")
    if promised > MAX_OBJECT:
        raise SystemExit(
            f"  {art.name}: the manifest says {remote['key']} is "
            f"{human(promised)}, past the {human(MAX_OBJECT)} litmo will "
            f"download for one object — {art.path} was not touched.\n"
            f"    Nothing is checked until the whole body is on disk, so a "
            f"number this large is not a bound at all.\n"
            f"    `litmo push {art.name}` republishes it with the real size.")

    print(f"  {art.name:9} downloading {human(promised)}"
          f" -> {art.path} ({remote.get('files', 0):,} files,"
          f" {human(remote.get('raw_bytes', 0))} unpacked)", flush=True)
    with _staging(ctx.cfg) as tmp:
        tmp = Path(tmp)
        bundle = tmp / "bundle.tar.zst"
        try:
            ctx.remote.download(remote["key"], bundle, promised)
        except Oversized as e:
            raise SystemExit(f"  {art.name}: {e} — {art.path} was not "
                             f"touched") from None
        got = file_sha256(bundle)
        if got != remote["archive_sha256"]:
            raise SystemExit(f"  {art.name}: archive failed verification — "
                             f"{art.path} was not touched\n"
                             f"    expected {remote['archive_sha256']}\n"
                             f"    got      {got}")

        tree = tmp / "tree"
        tree.mkdir()
        try:
            _unpack(bundle, tree)
        except SystemExit:
            raise
        except Exception as e:                                # noqa: BLE001
            raise SystemExit(f"  {art.name}: the bundle would not unpack "
                             f"({type(e).__name__}: {e}) — {art.path} was "
                             f"not touched") from None
        bundle.unlink()

        staged = tree.joinpath(*art.path.parts)
        # Every file under the artifact's own subtree is under `art.path` by
        # construction, so walking it to look for files *outside* `art.path`
        # can only ever answer no. Prune it: at 100,000 files that walk was
        # 2.9 s of a 19 s pull, and 0.00 s once pruned, for the same answer.
        # The predicate is still the one that decides, so a bundle that puts
        # the artifact somewhere unexpected is caught exactly as before.
        stray = []
        for dirpath, dirnames, filenames in os.walk(tree):
            here = Path(dirpath)
            if here == staged:
                dirnames[:] = []
                continue
            for name in filenames:
                rel = (here / name).relative_to(tree).as_posix()
                if ((here / name).is_file()
                        and not paths.is_under(rel, art.path.as_posix())):
                    stray.append(rel)
        after, n2, size2 = tree_hash(staged)
        if stray or after != remote["tree_hash"]:
            raise SystemExit(
                f"  {art.name}: the bundle is not the tree the manifest "
                f"describes — {art.path} was not touched\n"
                + (f"    it also holds {len(stray):,} file(s) outside "
                   f"{art.path}, e.g. {paths.display(stray[0])}\n"
                   if stray else "")
                + f"    expected {remote['tree_hash']}\n"
                  f"    got      {after}")

        # Verified: from here the working tree may be changed — and the
        # merge is checked for conflicts first, so `_install` cannot fail
        # halfway through and leave a tree that is neither copy.
        try:
            with _installing(ctx.cfg, art):
                merged = _install(staged, dest, clean=clean)
        except Blocked as b:
            raise _refuse_layout(
                art, b.conflicts, ctx.cfg.root, b.why,
                f"Remove the path(s) named, or re-run with --clean to "
                f"replace {art.path} with the bucket's copy.") from None

    # Only a merge can have left something the verification did not see. Any
    # other path renamed the tree that was just hashed into place, and a
    # rename cannot change its bytes — reading all of them back to say so was
    # another 3.0 s of that same 19 s pull.
    final, n3, size3 = (tree_hash(dest) if merged else (after, n2, size2))
    note = "verified" if final == remote["tree_hash"] else (
        "merged — local extras remain, so the tree is a superset of the "
        "bucket's (re-run with --clean for an exact copy)")
    print(f"  {art.name:9} {note}  ({n3:,} files, {human(size3)})")


def archive_push(ctx, art, *, force=False, dry_run=False) -> bool:
    src = _here(ctx, art)
    if not src.exists():
        print(f"  {art.name:9} skipped — {art.path} is not here")
        return False

    # Before the hash, not after: the walk is far cheaper than reading every
    # byte, and an artifact that cannot be published is worth saying so about
    # even on a push that would otherwise have found nothing to do.
    escaping, special = _unpublishable(src)
    if escaping:
        raise SystemExit(
            f"  {art.name}: {art.path} holds {len(escaping):,} symlink(s) "
            f"pointing outside it, which pack as links no reader can follow "
            f"— nothing was uploaded:\n"
            + _listing(art, escaping[:10], len(escaping))
            + f"\n    Replace them with the files themselves, or move the "
              f"targets inside {art.path}, then re-run `litmo push`.")
    if special:
        raise SystemExit(
            f"  {art.name}: {art.path} holds {len(special):,} file(s) that "
            f"are not files — a fifo or a device node packs into the bundle "
            f"and is then refused by every reader's extraction filter, while "
            f"`tree_hash` never counted it, so nothing else would ever have "
            f"said so — nothing was uploaded:\n"
            + _listing(art, special[:10], len(special))
            + f"\n    Remove them from {art.path}, then re-run `litmo push`.")

    digest, n, size = tree_hash(src)
    remote = _prior(ctx, art)
    if remote.get("tree_hash") == digest and not force:
        print(f"  {art.name:9} up to date  ({n:,} files, {human(size)})")
        return False
    # Compared again after packing; see the re-read below. Taken here rather
    # than beside `tree_hash` so a push with nothing to do pays for no extra
    # walk at all — the two are separated by a dict lookup, and anything
    # written in that gap and left there is caught by the digest anyway.
    stamped = stamps(src)

    print(f"  {art.name:9} packing     ({n:,} files, {human(size)}) …", flush=True)
    with _staging(ctx.cfg) as tmp:
        bundle = Path(tmp) / f"{art.name}.tar.zst"
        try:
            _pack(ctx.cfg.root, src, bundle)
        except Special as e:
            raise SystemExit(
                f"  {art.name}: {paths.display(e.rel)} became a fifo or a "
                f"device node while {art.path} was being packed — nothing "
                f"was uploaded\n"
                f"    No reader can extract one: the `data` filter refuses it "
                f"outright. And `tree_hash` does not count it, so the digest "
                f"re-checked below would have agreed and the next push would "
                f"have said `up to date`.\n"
                f"    Wait for whatever is writing {art.path} to finish, then "
                f"re-run `litmo push`.") from None
        except Escaping as e:
            raise SystemExit(
                f"  {art.name}: {paths.display(e.link)}"
                f" became a symlink "
                f"pointing outside {art.path} while it was being packed — "
                f"nothing was uploaded\n"
                f"    -> {paths.display(e.target)}\n"
                f"    A bundle carrying it is one no reader can extract, and "
                f"nothing after this point would have noticed: `tree_hash` "
                f"reads through a link, so the digest re-checked below is the "
                f"target's bytes either way.\n"
                f"    Wait for whatever is writing {art.path} to finish, then "
                f"re-run `litmo push`.") from None
        # `tree_hash` and `_pack` read the tree independently, and packing a
        # large one takes a while — long enough for a build still running
        # underneath the publish to put different bytes in each. The manifest
        # would then record a digest the bundle does not have, and every
        # reader's pull would fail on it until someone happened to push
        # again. Re-read before anything is uploaded.
        #
        # The re-read alone does not settle it, which is what the timestamps
        # are for: a file rewritten in place while `_pack` was reading it and
        # put back before this line leaves both hashes agreeing on bytes the
        # bundle does not hold. Reproduced — manifest `tree_hash` of a tree
        # of sixteen `A`, bundle holding sixteen `B`, push reporting success,
        # and the reader's pull then failing for good. Same window and the
        # same answer as `_unchanged`'s `stamp` on the mirror side.
        again, _, _ = tree_hash(src)
        if again != digest or stamps(src) != stamped:
            raise SystemExit(
                f"  {art.name}: {art.path} changed while it was being "
                f"packed — nothing was uploaded\n"
                + (f"    was {digest}\n"
                   f"    now {again}\n" if again != digest else
                   "    the bytes are back to what they were, but a file "
                   "was rewritten while the bundle was reading it, so the "
                   "bundle holds a mixture no reader could verify\n")
                + "    wait for whatever is writing it to finish, then "
                  "re-run `litmo push`")
        packed = bundle.stat().st_size
        if dry_run:
            print(f"  {art.name:9} would upload {human(packed)} -> {art.key}")
            return False
        print(f"  {art.name:9} uploading   {human(packed)}"
              f" ({100 * packed / max(size, 1):.0f}% of raw) -> {art.key}",
              flush=True)
        ctx.remote.upload(bundle, art.key, "application/zstd")
        ctx.manifest.set(art.name, {
            "kind": "archive",
            "key": art.key,
            "path": art.path.as_posix(),
            "tree_hash": digest,
            "archive_sha256": file_sha256(bundle),
            "archive_bytes": packed,
            "raw_bytes": size,
            "files": n,
        })
    return True


# ---------------------------------------------------------------- mirror ----
# File for file, with a sha256 for each. Slower to publish than a bundle, but a
# reader can fetch one file without the rest, and a re-publish after a partial
# re-run uploads only what actually moved.

def _covers(art, rel: str) -> bool:
    """Is `rel` a path this artifact's own rules cover?

    One test, asked of both sides. The local index has always applied it; the
    manifest's list did not, and a filter applied to one side only is a pull
    and a status that can never agree — every pull downloads a file the local
    index will not look at, and every status calls it missing again.
    """
    p = PurePosixPath(rel)
    inside = p.parts[len(art.path.parts):]
    if not inside:
        return True                     # the artifact is that one file
    if p.name.endswith(".part") or SKIP_DIRS & set(inside):
        return False
    return not art.include or p.suffix in art.include


def _walk(root: Path, art) -> list[Path]:
    base = root / art.path
    if base.is_file():
        return [base]
    if not base.exists():
        return []
    return [p for p in sorted(base.rglob("*"))
            if p.is_file() and _covers(art, p.relative_to(root).as_posix())]


def _remote_index(ctx, art) -> list[dict]:
    """The manifest's files for this artifact, less the ones it does not cover.

    A bucket can hold entries outside the filter — a legacy flat list, which
    the manifest reader takes transparently, or an `include` narrowed since
    the last push. They belong to nothing this repository publishes, so pull
    and status leave them where they are.

    A path that is not a well-formed relative path is not filtered but kept,
    so that `_dest` refuses it by name. Quietly dropping it instead would turn
    a manifest nobody should act on into one that looks merely small.
    """
    def judgeable(rel) -> bool:
        try:
            paths.relative(rel)
        except paths.Unsafe:
            return False
        return True

    return [e for e in (_entry(ctx, art) or {}).get("files", [])
            if not judgeable(e.get("path")) or _covers(art, e["path"])]


def _local_index(ctx, art, *, workers=8) -> tuple[dict[str, dict], int]:
    """Every covered local file, with its size and its digest.

    Every mirror status, pull and push reads and hashes the whole local tree
    before it can say that nothing moved, so this is the floor under a no-op
    command. The files are independent and the cost is their bytes, so the
    reads run `workers` wide — the same width the mirror commands already
    give their transfers, and the same flag. Measured on the largest real
    mirror any consumer here publishes, 711 files and 2.18 GB: 1.30 s serial
    against 0.28 s at eight warm, 3.09 s against 0.84 s with the page cache
    dropped underneath it.

    The index is still assembled in the sorted order `_walk` returns, because
    that order is what `_classify` hands to the sweep and what `mirror_push`
    uploads in. The names are checked first, all of them, before a byte is
    read: an unpublishable one then refuses the command by the same path
    every time rather than by whichever worker reached it.
    """
    files = []
    for p in _walk(ctx.cfg.root, art):
        rel = p.relative_to(ctx.cfg.root).as_posix()
        # A name that cannot be written into the manifest cannot be published:
        # the next reader would refuse the whole document over one file.
        try:
            paths.relative(rel, what=f"artifact {art.name!r}")
        except paths.Unsafe as e:
            raise SystemExit(f"  {e}\n  rename it, or exclude it with "
                             f"`include`, before publishing") from None
        files.append((p, rel))

    def measure(p: Path) -> tuple[int, str]:
        return p.stat().st_size, file_sha256(p)

    pool = cf.ThreadPoolExecutor(max_workers=max(1, workers))
    try:
        # Collected in submission order, so a file that has gone missing
        # underneath the walk still stops the command promptly; the queued
        # reads are dropped rather than each finishing first, the same
        # reasoning as `download_many`.
        measured = [f.result()
                    for f in [pool.submit(measure, p) for p, _ in files]]
    finally:
        pool.shutdown(wait=True, cancel_futures=True)

    idx, total = {}, 0
    for (_, rel), (size, digest) in zip(files, measured, strict=True):
        idx[rel] = {"path": rel, "size": size, "sha256": digest}
        total += size
    return idx, total


def _unchanged(path: Path, e: dict, stamp: int | None = None) -> bool:
    """Is the file still the bytes `_local_index` hashed into `e`?

    Size first, because it settles nearly every real case without re-reading
    a large file.

    `stamp` is `st_mtime_ns` read before something else read the file, and is
    checked as well as the content. Content alone cannot see a write that was
    undone. `mirror_push` uploads from the live working-tree path, and
    `upload_file` reads that path in parts — and re-reads a part it retries —
    so a writer rewriting the file in place between two of those reads puts
    different generations into different parts of one object. Put the
    original bytes back before the re-read and the re-read agrees. Reproduced
    with two eight-byte halves: the object was `AAAAAAAABBBBBBBB` while the
    manifest recorded the digest of sixteen `A`, and the push reported
    success. An ordinary writer cannot restore `st_mtime_ns` as well as the
    bytes — 200 back-to-back in-place rewrites here produced 200 distinct
    values — so bracketing the upload with it closes the window.

    Only ever grounds to refuse: nothing is accepted because of a timestamp,
    so this can cost a push to a bare `touch` but can never publish a digest
    the bucket does not hold.
    """
    try:
        st = path.stat()
        return (st.st_size == e["size"]
                and (stamp is None or st.st_mtime_ns == stamp)
                and file_sha256(path) == e["sha256"])
    except OSError:
        return False


def _classify(local: dict, remote: list[dict]) -> tuple[list, list, list]:
    """(missing, stale, extra) — what pull would need, from the reader's side."""
    missing, stale = [], []
    for e in remote:
        got = local.get(e["path"])
        if got is None:
            missing.append(e)
        elif got["sha256"] != e["sha256"]:
            stale.append(e)
    known = {e["path"] for e in remote}
    extra = [p for p in local if p not in known]
    return missing, stale, extra


def mirror_status(ctx, art) -> Report:
    local, total = _local_index(ctx, art)
    remote = _remote_index(ctx, art)
    # An entry holding no covered files is the bucket saying it has nothing
    # here — a different fact from never having been pushed, and the one
    # `pull --clean` acts on. `_remote_index` is empty for both, so keying the
    # verdict off it called a withdrawn mirror "local only" and sent a reader
    # looking for a bucket that was answering perfectly well. `archive_status`
    # already reads the entry rather than a derived list, for the same reason.
    published = _entry(ctx, art) is not None
    local_s = f"{len(local):,} files, {human(total)}" if local else "absent"
    remote_s = (f"{len(remote):,} files, "
                f"{human(sum(e['size'] for e in remote))}" if remote else
                "0 files" if published else "absent")
    if not published:
        return Report(local_s, remote_s, LOCAL_ONLY if local else ABSENT)
    if not local:
        # Nothing here, and nothing published either, is agreement — not the
        # absence of anything to compare.
        return Report(local_s, remote_s, IN_SYNC if not remote else REMOTE_ONLY)
    missing, stale, extra = _classify(local, remote)
    if not (missing or stale or extra):
        return Report(local_s, remote_s, IN_SYNC)
    detail = ", ".join(f"{len(x)} {n}" for n, x in
                       (("missing", missing), ("stale", stale), ("extra", extra)) if x)
    return Report(local_s, remote_s, f"{DIFFERS} ({detail})")


def mirror_pull(ctx, art, *, force=False, clean=False, workers=8) -> None:
    remote = _remote_index(ctx, art)
    entry = _entry(ctx, art)
    if not remote and entry is None:
        # The manifest has never described this artifact, so there is nothing
        # here to bring the local tree into line with — not even emptiness.
        # `--clean` in particular must not sweep on the strength of silence.
        print(f"  {art.name:9} not in the bucket")
        return
    local, _ = _local_index(ctx, art, workers=workers)
    missing, stale, extra = _classify(local, remote)
    want = remote if force else missing + stale

    def sweep() -> None:
        """Remove the local files the bucket does not have.

        The only irreversible thing a pull does, and artifact directories are
        git-ignored, so the copy being removed is routinely the only one.
        That is why it happens *after* the download has verified and never
        before: a pull that fails deletes nothing, which is what the module
        docstring promises and what the archive kind already does by sweeping
        inside `_install`.
        """
        for rel in extra:
            print(f"  {art.name:9} removing local extra {rel}")
            with contextlib.suppress(FileNotFoundError):
                (ctx.cfg.root / rel).unlink()

    if not remote:
        # The bucket describes this artifact and describes it as holding
        # nothing — the publisher deleted its last covered file, or `include`
        # no longer covers what was published. That is a state a reader can be
        # brought into sync with, so `--clean` applies it rather than treating
        # the artifact as absent; without `--clean` the local files stay put,
        # exactly as any other local extra would.
        why = ("nothing in the bucket is covered by `include`"
               if entry.get("files") else "the bucket holds no files here")
        if clean:
            sweep()                    # prints a line per file it removes
        elif extra:
            why += f"  ({len(extra):,} kept — `pull --clean` removes them)"
        print(f"  {art.name:9} {why}")
        return
    if not want:
        if clean:
            sweep()                    # prints a line per file it removes
        # What is left, not what was here when the comparison ran: `sweep`
        # has just deleted the extras, and counting them as still present
        # made a `--clean` pull report more files than it had left behind.
        left = len(local) - len(extra) if clean else len(local)
        print(f"  {art.name:9} up to date  ({left:,} files)")
        return
    # The same promise, and the same reason to distrust it: each file's only
    # bound is the size the manifest gives it, so one entry claiming an
    # absurd number streams until the disk fills before a digest is looked at.
    if huge := [e for e in want if e["size"] > MAX_OBJECT]:
        raise SystemExit(
            f"  {art.name}: the manifest says {huge[0]['path']} is "
            f"{human(huge[0]['size'])}, past the {human(MAX_OBJECT)} litmo "
            f"will download for one object — nothing under {art.path} was "
            f"changed.\n"
            + (f"    … and {len(huge) - 1:,} more like it\n"
               if len(huge) > 1 else "")
            + "    Nothing is checked until the whole body is on disk, so a "
              "number this large is not a bound at all.\n"
            + f"    `litmo push {art.name}` republishes it with the real "
              f"sizes.")
    print(f"  {art.name:9} downloading {len(want):,} of {len(remote):,} files"
          f"  ({human(sum(e['size'] for e in want))})", flush=True)

    with _staging(ctx.cfg) as tmp:
        tmp = Path(tmp)
        moves = [(e, tmp.joinpath(*PurePosixPath(e["path"]).parts),
                  _dest(ctx, art, e["path"])) for e in want]
        try:
            ctx.remote.download_many(
                [(e["path"], staged, e["size"]) for e, staged, _ in moves],
                workers=workers)
        except Oversized as e:
            raise SystemExit(f"  {art.name}: {e} — nothing under {art.path} "
                             f"was changed") from None

        # Every staged file is read again here, in full — this is the gate
        # that decides whether the working tree may be touched at all, so it
        # runs `workers` wide like the download that filled the staging tree
        # and like `_local_index` before it. On the largest real mirror
        # published from here, 711 files and 2.18 GB, that is 2.25 s serial
        # against 0.55 s at eight warm (3.26 s against 0.96 s cold). Every
        # file is checked, never just up to the first failure, because the
        # refusal names them; and `bad` keeps `moves` order, so the ten it
        # prints are the same ten every time.
        def verified(item) -> bool:
            e, staged, _ = item
            return (staged.exists() and staged.stat().st_size == e["size"]
                    and file_sha256(staged) == e["sha256"])

        pool = cf.ThreadPoolExecutor(max_workers=max(1, workers))
        try:
            ok = [f.result() for f in [pool.submit(verified, m) for m in moves]]
        finally:
            pool.shutdown(wait=True, cancel_futures=True)
        bad = [e["path"] for (e, _s, _d), good in zip(moves, ok, strict=True)
               if not good]
        if bad:
            raise SystemExit(
                f"  {art.name}: {len(bad)} of {len(want)} file(s) failed "
                f"verification — nothing under {art.path} was changed:\n" +
                "\n".join(f"    {p}" for p in bad[:10]) +
                (f"\n    … and {len(bad) - 10:,} more" if len(bad) > 10 else ""))

        here = _here(ctx, art).resolve()
        blocked = [c for _e, _s, d in moves if (c := _blocked(d, here))]
        if blocked:
            raise _refuse_layout(art, blocked, ctx.cfg.root,
                                 "installing would fail partway",
                                 "Remove the path(s) named and pull again.")

        # Verified, and every destination can be written: from here the
        # working tree may be changed. The layout check comes before both of
        # the steps below, so a refusal costs nothing at all.
        for _e, staged, dest in moves:
            dest.parent.mkdir(parents=True, exist_ok=True)
            _move(staged, dest)
        # After the installs, not before. An `extra` is a local-only file —
        # the bucket does not have it, by construction — so sweeping first
        # and then failing partway through the loop above deleted the one
        # thing in the artifact that re-running the pull cannot bring back.
        # Reproduced with a three-file `--clean` pull failing EACCES on its
        # second `_move`: `out/extra.csv` was already gone, `a.csv` held the
        # published bytes and `b.csv`/`c.csv` the local ones. Everything a
        # failed install leaves behind now is one whole generation or the
        # other, and re-running the pull finishes the switch.
        #
        # Ordering is otherwise free: `extra` cannot name a file the loop
        # installs, and anything standing in the loop's way was refused by
        # `_blocked` above rather than swept out of it.
        if clean:
            sweep()
    print(f"  {art.name:9} {len(want):,} fetched and verified -> {art.path}")


def _mirror_entry(art, local: dict, known: dict, uploaded: set,
                  sent: set) -> dict:
    """What the bucket holds, as best this push knows.

    A file is listed with its local digest if it was uploaded and re-read
    unchanged, or was already current. One whose upload never started keeps
    the digest the bucket had before, because that is still what is there.
    One with neither is not in the bucket at all, and so is not listed.
    Building the entry this way is what lets the manifest be written even
    when a push dies partway: it describes what happened rather than what
    was intended.

    The fourth case is why `sent` is tracked apart from `uploaded`: a file
    whose PUT *returned* but whose re-read then disagreed — because the
    pipeline rewrote it, or because the run was interrupted before the
    re-read finished — has moved the object to bytes this push cannot name.
    Its previous digest is no longer what is there, and its local digest was
    never what went up, so listing either would put a digest in the manifest
    that the bucket does not hold and fail every reader's pull. It is left
    out, and the next push re-uploads it.
    """
    files = []
    for rel in sorted(local):
        e = local[rel]
        if rel in sent and rel not in uploaded:
            continue
        if rel in uploaded or known.get(rel, {}).get("sha256") == e["sha256"]:
            files.append(e)
        elif rel in known:
            files.append(known[rel])
    return {"kind": "mirror", "path": art.path.as_posix(),
            "raw_bytes": sum(f["size"] for f in files), "files": files}


def mirror_push(ctx, art, *, force=False, dry_run=False,
                workers=8) -> bool:
    base = _here(ctx, art)
    local, total = _local_index(ctx, art, workers=workers)
    # An empty directory is a publishable state — it is how deleting the last
    # file reaches the bucket. A missing one is not: `out/` not existing on
    # this machine means the pipeline has not run here, not that the artifact
    # is now empty.
    if not local and not base.exists():
        print(f"  {art.name:9} skipped — {art.path} is not here")
        return False
    known = {e["path"]: e for e in _prior(ctx, art).get("files", [])}
    todo = [e for e in local.values()
            if force or known.get(e["path"], {}).get("sha256") != e["sha256"]]
    gone = sorted(known.keys() - local.keys())
    # Two different things, and saying "no longer here" about a file that is
    # sitting right there sends someone looking for a deletion that did not
    # happen. A path the artifact does not cover was never in the local index.
    deleted = [rel for rel in gone if _covers(art, rel)]
    uncovered = [rel for rel in gone if not _covers(art, rel)]

    print(f"  {art.name:9} {len(local):,} files, {human(total)} — "
          f"{len(todo):,} changed, {len(local) - len(todo):,} already current")
    if deleted:
        print(f"  {art.name:9} {len(deleted):,} no longer here — dropping from "
              f"the manifest (the objects stay in the bucket)")
    if uncovered:
        print(f"  {art.name:9} {len(uncovered):,} no longer covered by "
              f"`include` — dropping from the manifest (the objects stay in "
              f"the bucket)")
    if dry_run:
        for e in todo[:20]:
            print(f"    {e['size']:>12,}  {e['path']}")
        if len(todo) > 20:
            print(f"    … and {len(todo) - 20:,} more")
        for rel in gone[:20]:
            print(f"    {'drop':>12}  {rel}")
        return False

    uploaded: set[str] = set()      # the PUT returned *and* the bytes held
    sent: set[str] = set()          # the PUT returned; the object has moved
    raced: list[str] = []
    # `sent` is added to by the worker the instant its PUT returns, under a
    # lock, and not by the main thread when it gets round to the result.
    # Whatever ends the push, the manifest is written from these two sets, and
    # a file whose object has already moved has to be in `sent` by then — a
    # result still sitting in an uncollected future would otherwise leave the
    # manifest naming the digest the bucket held *before*, which is the one
    # thing `_mirror_entry` exists to avoid.
    landed = threading.Lock()

    def send(e: dict) -> tuple[dict, bool]:
        full = ctx.cfg.root / e["path"]
        ctype = mimetypes.guess_type(str(full))[0] or "application/octet-stream"
        # Bracketing the upload, not just following it: the window that
        # matters is the upload's own reads of the live path, and a rewrite
        # undone before the re-read below is invisible to the re-read. See
        # `_unchanged`. A file that has gone missing leaves `stamp` None and
        # is caught by the re-read either way.
        try:
            stamp = full.stat().st_mtime_ns
        except OSError:
            stamp = None
        ctx.remote.upload(full, e["path"], ctype)
        with landed:
            sent.add(e["path"])
        # The digest was taken before the upload started, so a file
        # rewritten in between would be published under a digest the
        # object does not have — and every reader would then fail to
        # verify it. Re-reading the file is the only way to know that
        # the bytes which went up are the bytes being described.
        return e, _unchanged(full, e, stamp)

    # One PUT is one round trip, and doing them one after another made a push
    # take as long as the bucket is far away: 200 objects at 50 ms was 10.1 s
    # serial against 1.3 s for the pull of the same files, which has run eight
    # wide through `download_many` all along. Same width, same `--workers`.
    futures: list[cf.Future] = []
    pool = cf.ThreadPoolExecutor(max_workers=max(1, workers))
    try:
        futures = [pool.submit(send, e) for e in todo]
        for i, fut in enumerate(cf.as_completed(futures), 1):
            e, held = fut.result()
            if not held:
                raced.append(e["path"])
                print(f"    [{i}/{len(todo)}] {'changed':>12}  {e['path']}"
                      f"  — rewritten mid-upload", flush=True)
                continue
            uploaded.add(e["path"])
            print(f"    [{i}/{len(todo)}] {e['size']:>12,}  {e['path']}", flush=True)
    finally:
        # A failed upload is the answer for the whole push, so the queued ones
        # are dropped rather than each spending its own socket timeout — the
        # same reasoning as `download_many`.
        pool.shutdown(wait=True, cancel_futures=True)
        # The ones that finished while the main thread was busy with a failure
        # elsewhere still landed and still verified. Collect them, or a push
        # that dies at eight-wide throws away up to seven good uploads the
        # serial loop would have recorded.
        for fut in futures:
            if fut.done() and not fut.cancelled() and not fut.exception():
                e, held = fut.result()
                if held:
                    uploaded.add(e["path"])
        # Even an interrupted push leaves the manifest describing the objects
        # that did land; `cli.cmd_push` commits it on the way out. A file
        # whose PUT returned but did not verify — including one interrupted
        # between the two — is dropped rather than left with a digest that is
        # no longer in the bucket. Either way the next push sees it as changed
        # and re-uploads it.
        ctx.manifest.set(art.name,
                         _mirror_entry(art, local, known, uploaded, sent))
    if raced:
        raise SystemExit(
            f"  {art.name}: {len(raced)} file(s) were rewritten while they "
            f"were uploading, so the bucket now holds bytes this push cannot "
            f"describe:\n" +
            "\n".join(f"    {p}" for p in raced[:10]) +
            (f"\n    … and {len(raced) - 10:,} more" if len(raced) > 10 else "") +
            f"\n    They are not published. Wait for whatever is writing "
            f"{art.path} to finish, then re-run `litmo push`.")
    # Even with nothing uploaded the file list may have shrunk, so the manifest
    # is rewritten whenever it no longer matches what is on disk.
    return bool(todo) or bool(gone)


# ----------------------------------------------------------------- fetch ----
# Inputs published by something outside this repository. There is no manifest
# to compare against, so freshness is the server's ETag, remembered locally.

def _intact(dest: Path, seen: dict) -> bool:
    """Is the local copy still the bytes that were downloaded into it?

    Without this an edited file whose upstream ETag has not moved reads as in
    sync forever, and `pull` never repairs it. Size is compared first because
    it settles nearly every real case without hashing a large input.
    """
    if not seen.get("sha256"):
        return True                    # nothing recorded — a pre-0.1 state file
    try:
        if seen.get("size") is not None and dest.stat().st_size != seen["size"]:
            return False
        return file_sha256(dest) == seen["sha256"]
    except OSError:
        return False


def _fresh(now: dict, seen: dict) -> bool:
    """Is the server still serving the bytes `seen` was recorded from?

    Every validator both sides carry has to agree. An OR here — fresh if
    *either* one matches — let a moved ETag lose to a `Last-Modified` that
    had not moved, and that is the common shape rather than a corner: the
    ETag is derived from the bytes, while `Last-Modified` is one-second
    resolution at best and plenty of publishers rewrite an object without
    advancing it. The pair (new etag, unchanged Last-Modified) then read as
    "up to date" forever, so `pull` never re-fetched and `status` reported a
    changed input as in sync.

    A validator only one side has is skipped, not failed: nothing can be
    concluded from comparing it. If that leaves nothing to compare, the file
    is not fresh — re-fetching is the safe answer when the server offers no
    way to tell.
    """
    agree = [now[k] == seen.get(k) for k in ("etag", "last_modified")
             if now.get(k) and seen.get(k)]
    return bool(agree) and all(agree)


def fetch_status(ctx, art) -> Report:
    dest = _here(ctx, art)
    seen = ctx.state.get("fetch", {}).get(art.name, {})
    here = dest.exists()
    intact = _intact(dest, seen) if here else True
    local_s = "absent"
    if here:
        local_s = human(dest.stat().st_size)
        if not intact:
            local_s += ", modified here"
        elif seen.get("last_modified"):
            local_s += ", " + seen["last_modified"]
    try:
        now = head(art.url)
    except Exception as exc:                                  # noqa: BLE001
        return Report(local_s, f"unreachable ({type(exc).__name__})", DIFFERS)
    remote_s = (human(int(now["size"])) if now.get("size") else "?") + \
               (", " + now["last_modified"] if now.get("last_modified") else "")
    if not here:
        return Report(local_s, remote_s, REMOTE_ONLY)
    return Report(local_s, remote_s,
                  IN_SYNC if (intact and _fresh(now, seen)) else DIFFERS)


def fetch_pull(ctx, art, *, force=False, clean=False) -> None:
    dest = _here(ctx, art)
    seen = ctx.state.setdefault("fetch", {}).get(art.name, {})
    if dest.exists() and not force:
        if not _intact(dest, seen):
            print(f"  {art.name:9} local copy is not what was fetched — "
                  f"replacing it")
        else:
            try:
                if _fresh(head(art.url), seen):
                    print(f"  {art.name:9} up to date  "
                          f"({human(dest.stat().st_size)})")
                    return
            except Exception:                                 # noqa: BLE001
                pass      # a server that will not answer HEAD still answers GET

    print(f"  {art.name:9} fetching    {art.url}", flush=True)
    try:
        meta = fetch_url(art.url, dest)
    except Oversized as e:
        raise SystemExit(f"  {art.name}: {art.url} is {e} — {art.path} was "
                         f"not touched") from None
    meta |= {"sha256": file_sha256(dest), "size": dest.stat().st_size}
    ctx.state["fetch"][art.name] = meta
    print(f"  {art.name:9} {human(meta['size'])} -> {art.path}")


def fetch_push(ctx, art, *, force=False, dry_run=False) -> bool:
    print(f"  {art.name:9} not published from here — {art.url} is written by "
          f"something else")
    return False


# -------------------------------------------------------------- dispatch ----

VERBS = {
    "archive": (archive_status, archive_pull, archive_push),
    "mirror": (mirror_status, mirror_pull, mirror_push),
    "fetch": (fetch_status, fetch_pull, fetch_push),
}


def status(ctx, art) -> Report:
    return VERBS[art.kind][0](ctx, art)


def pull(ctx, art, **kw) -> None:
    VERBS[art.kind][1](ctx, art, **kw)


def push(ctx, art, **kw) -> bool:
    return VERBS[art.kind][2](ctx, art, **kw)


# ------------------------------------------------------------------ state ---
# Only the `fetch` kind needs local memory; the others compare against the
# manifest, which is the same everywhere.

def load_state(cfg) -> dict:
    p = cfg.state_file
    if p.exists():
        try:
            state = json.loads(p.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError, OSError):
            return {}
        return state if isinstance(state, dict) else {}
    return {}


def save_state(cfg, state: dict) -> None:
    """Write via a sibling and rename, so an interrupted write loses the
    update rather than the file."""
    p = cfg.state_file
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_name(p.name + ".part")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, p)
