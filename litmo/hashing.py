"""Content identity — how litmo decides whether two copies are the same.

`tree_hash` and `file_sha256` look at nothing but names and bytes — not
mtimes, not ownership, not the machine. Two checkouts holding the same bytes
under the same names agree, and a `push` after a no-op re-run uploads
nothing.

`stamps` is the one thing here that reads a timestamp, and it is deliberately
not identity: nothing is ever called unchanged because of it. It exists so a
caller that reads a tree twice can tell that something rewrote it in between
and put the bytes back, which is invisible to a digest by construction.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

CHUNK = 1 << 20


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(CHUNK), b""):
            h.update(chunk)
    return h.hexdigest()


def _files(root: Path) -> tuple[list[Path], Path]:
    """The files `tree_hash` reads, and the directory their names are
    relative to. Shared so `stamps` cannot drift out of step with it."""
    files = ([root] if root.is_file()
             else sorted(p for p in root.rglob("*") if p.is_file()))
    return files, (root.parent if root.is_file() else root)


def stamps(root: Path) -> dict[str, tuple[int, int]]:
    """(size, st_mtime_ns) for exactly the files `tree_hash` reads.

    A tripwire, not identity. `archive_push` hashes the tree, packs it, and
    hashes it again to catch a build still writing underneath the publish —
    but a file rewritten in place while the bundle was reading it and put
    back before the second hash leaves both hashes agreeing on bytes the
    bundle does not contain. Reproduced: the manifest recorded the digest of
    a tree of sixteen `A`, the bundle held sixteen `B`, the push reported
    success, and every reader's pull then failed with "the bundle is not the
    tree the manifest describes".

    An ordinary writer cannot put `st_mtime_ns` back as well as the bytes.
    This can only ever be grounds to refuse a push, so the worst it costs is
    a push lost to a `touch`.
    """
    if not root.exists():
        return {}
    files, base = _files(root)
    out = {}
    for p in files:
        st = p.stat()
        out[p.relative_to(base).as_posix()] = (st.st_size, st.st_mtime_ns)
    return out


def tree_hash(root: Path) -> tuple[str, int, int]:
    """(hash, file count, total bytes) for a file or a directory.

    Paths are sorted, so the result does not depend on directory iteration
    order, and only names and contents are hashed. This is deliberately not a
    hash of the packed archive: zstd is not bit-reproducible across versions,
    so hashing the archive would make an unchanged corpus look changed on a
    different machine.
    """
    if not root.exists():
        return ("", 0, 0)
    files, base = _files(root)

    h = hashlib.sha256()
    total = 0
    for p in files:
        h.update(p.relative_to(base).as_posix().encode("utf-8"))
        h.update(b"\0")
        h.update(file_sha256(p).encode("ascii"))
        h.update(b"\n")
        total += p.stat().st_size
    return (h.hexdigest(), len(files), total)


def human(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024 or unit == "GB":
            return f"{n:,.0f} {unit}" if unit == "B" else f"{n:,.1f} {unit}"
        n /= 1024
    return f"{n} B"
