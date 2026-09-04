"""litmo — move a repository's data and artifacts between a clone and a bucket.

    litmo pull [name...]      bucket -> here   (what `make sync` runs)
    litmo push [name...]      here -> bucket   (what `make publish` runs)
    litmo status [name...]    compare the two; exits non-zero if they differ
    litmo mk                  print the shared Makefile to stdout
    litmo doctor              check the toolchain, the config and the creds

What each repository publishes is declared in its own `sync.toml`; see
litmo.config for the schema. Credentials, when a command needs them, come
from `.r2` or the environment; see litmo.creds.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import urllib.error
from pathlib import Path

from . import config, kinds
from .manifest import Manifest

# `doctor` exists for the machine whose toolchain is broken, which is also the
# machine where `quarto --version` can stall — a first run initialising its
# cache against an unreachable home directory, a wedged mount, a wrapper
# script waiting on something. It has to come back and say so.
QUARTO_TIMEOUT = 10


class Ctx:
    """Config, transport and manifest — the last two built only if used.

    A repository whose inputs are all `fetch` (published by something else)
    never touches a bucket, and should not need credentials or boto3 to run
    `litmo pull`.
    """

    def __init__(self, cfg, *, need_write: bool = False):
        self.cfg = cfg
        self.state = kinds.load_state(cfg)
        self._need_write = need_write
        self._remote = None
        self._manifest = None

    @property
    def remote(self):
        if self._remote is None:
            from .remote import Remote
            self._remote = Remote(self.cfg, need_write=self._need_write)
        return self._remote

    @property
    def manifest(self) -> Manifest:
        if self._manifest is None:
            self._manifest = Manifest.load(self.remote, self.cfg)
        return self._manifest

    @property
    def manifest_loaded(self) -> bool:
        """Whether asking for `.manifest` would cost a round trip. Used on the
        way out of a failed push, where loading it would only mask the error
        that got us there."""
        return self._manifest is not None

    def save(self) -> None:
        kinds.save_state(self.cfg, self.state)


def _banner(ctx, arts) -> None:
    if any(a.kind != "fetch" for a in arts):
        print(f"  bucket {ctx.remote.where}"
              f"{'  (public, no credentials)' if ctx.remote.public else ''}")


# --- commands ---------------------------------------------------------------

def cmd_pull(args) -> int:
    cfg = config.load()
    ctx = Ctx(cfg)
    arts = cfg.select(args.artifacts)
    _banner(ctx, arts)
    rc = 0
    # An empty bucket is a reason to skip the artifacts that come out of it,
    # not a reason to skip the `fetch` inputs, which have nothing to do with
    # it. Report it, drop those artifacts, and carry on with the rest.
    if any(a.kind != "fetch" for a in arts) and ctx.manifest.empty:
        skipped = [a for a in arts if a.kind != "fetch"]
        print(f"  {ctx.remote.where} holds nothing yet — run `litmo push` "
              f"from a machine that has the data")
        print(f"  skipping {', '.join(a.name for a in skipped)}")
        arts = [a for a in arts if a.kind == "fetch"]
        rc = 1
    try:
        for art in arts:
            kw = {"force": args.force, "clean": args.clean}
            if art.kind == "mirror":
                kw["workers"] = args.workers
            kinds.pull(ctx, art, **kw)
    finally:
        # What did land is remembered even if a later artifact failed, so a
        # re-run does not re-download it.
        ctx.save()
    return rc


def cmd_push(args) -> int:
    cfg = config.load()
    ctx = Ctx(cfg, need_write=True)
    arts = cfg.select(args.artifacts)
    _banner(ctx, arts)
    try:
        for art in arts:
            kw = {"force": args.force, "dry_run": args.dry_run}
            if art.kind == "mirror":
                kw["workers"] = args.workers
            kinds.push(ctx, art, **kw)
    finally:
        # Written last, once, and from a `finally`: objects go to the bucket
        # under their own names, so a push that dies partway has already
        # changed it. Committing here means the manifest describes what
        # actually landed rather than a state that no longer exists. The
        # If-Match is against the copy this run read, so a second maintainer
        # publishing in the meantime is a refusal rather than a silent loss.
        if not args.dry_run and ctx.manifest_loaded and ctx.manifest.dirty:
            ctx.remote.put_bytes(cfg.manifest_key, ctx.manifest.dump(),
                                 "application/json",
                                 if_match=ctx.manifest.etag,
                                 if_absent=not ctx.manifest.existed)
            print(f"  wrote {cfg.manifest_key}")
        elif not args.dry_run:
            print("  nothing to publish")
    return 0


def cmd_status(args) -> int:
    cfg = config.load()
    ctx = Ctx(cfg)
    # Manual artifacts are listed here even though a bare pull or push skips
    # them:
    # status moves nothing, and an input that has drifted upstream is exactly
    # what you want this command to tell you.
    arts = cfg.select(args.artifacts, include_manual=True)
    _banner(ctx, arts)
    print(f"\n  {'artifact':10} {'local':>24}  {'remote':>24}  state")
    print(f"  {'-' * 10} {'-' * 24}  {'-' * 24}  {'-' * 20}")
    rc = 0
    for art in arts:
        r = kinds.status(ctx, art)
        rc |= 0 if r.ok else 1
        print(f"  {art.name:10} {r.local:>24}  {r.remote:>24}  {r.verdict}")
    print()
    for art in arts:
        print(f"  {art.name:10} {art.kind:8} {art.path}"
              f"{'  [manual]' if art.manual else ''}"
              f"{'  — ' + art.what if art.what else ''}")
    if any(a.manual for a in arts):
        print("\n  [manual] artifacts are skipped by a bare pull or push — name "
              "one to move it:\n    uv run litmo pull "
              f"{next(a.name for a in arts if a.manual)}")
    ctx.save()
    return rc


def cmd_mk(args) -> int:
    sys.stdout.write(mk_text())
    return 0


def cmd_doctor(args) -> int:
    ok = True

    def check(label: str, good: bool, detail: str = "",
              fatal: bool = True) -> None:
        """`fatal=False` reports without failing — for things that are a
        limitation rather than a fault, like having no credentials in a
        checkout that only ever reads from a public bucket."""
        nonlocal ok
        ok &= good or not fatal
        mark = "ok  " if good else ("FAIL" if fatal else "note")
        print(f"  {mark}  {label:22} {detail}")

    print(f"  litmo {__import__('litmo').__version__}\n")
    check("python", sys.version_info >= (3, 12),
          ".".join(map(str, sys.version_info[:3])))
    q = shutil.which("quarto")
    check("quarto", bool(q), q or "not on PATH — https://quarto.org/docs/download/")
    u = shutil.which("uv")
    check("uv", bool(u), u or "not on PATH")

    try:
        cfg = config.load()
    except SystemExit as e:
        check("sync.toml", False, str(e).splitlines()[0])
        return 1
    check("sync.toml", True,
          f"{cfg.root / 'sync.toml'} — {len(cfg.artifacts)} artifact(s)")

    # Read and write readiness are separate questions: a contributor who only
    # ever runs `make sync` against a public bucket needs no credentials, and
    # a doctor that calls that a failure is telling them to go find some.
    needs_bucket = [a for a in cfg.artifacts if a.kind != "fetch"]
    if not needs_bucket:
        check("read access", True, "not needed — every artifact is a `fetch`")
        check("write access", True, "not needed — every artifact is a `fetch`")
    else:
        from . import creds
        try:
            creds.load(cfg.root, cfg.bucket)
            have_creds, why = True, "credentials present"
        except SystemExit as e:
            have_creds, why = False, str(e).splitlines()[0]
        check("read access", bool(cfg.base) or have_creds,
              f"{cfg.base} — public, no credentials needed" if cfg.base
              else ("over the S3 API" if have_creds else why))
        # Publishing is the maintainer's job. Not being set up for it is worth
        # saying and not worth failing over, as long as reads work.
        check("write access", have_creds,
              why if have_creds else f"{why} — pull works, push will not",
              fatal=not cfg.base)

    vendored = cfg.root / "common.mk"
    if vendored.exists():
        same = vendored.read_text(encoding="utf-8") == mk_text()
        check("common.mk", same,
              "matches this litmo" if same
              else "differs from this litmo — refresh with `make mk-update`")
    else:
        check("common.mk", False, "missing — `litmo mk > common.mk`")

    if q:
        try:
            v = subprocess.run([q, "--version"], capture_output=True,
                               text=True, timeout=QUARTO_TIMEOUT)
            if v.returncode:
                # On PATH is not the same as working. A missing shared
                # library, a broken wrapper or a half-finished install all
                # answer `--version` with a non-zero exit and nothing on
                # stdout, and this printed `quarto ` and exited 0 — a green
                # doctor in front of a render that cannot start, from the one
                # command whose whole job is to say what is wrong.
                print(f"\n  FAIL  quarto {q} exited {v.returncode} from "
                      f"`--version` — on PATH, but not working")
                for line in (v.stderr or v.stdout).strip().splitlines()[:3]:
                    print(f"          {line}")
                ok = False
            else:
                print(f"\n  quarto {v.stdout.strip()}")
        except subprocess.TimeoutExpired:
            print(f"\n  FAIL  quarto {q} did not answer `--version` within "
                  f"{QUARTO_TIMEOUT}s — wedged, not missing")
            ok = False
    return 0 if ok else 1


# --- the shared Makefile ----------------------------------------------------

def mk_text() -> str:
    return (Path(__file__).parent / "data" / "common.mk").read_text(encoding="utf-8")


# --- entry point ------------------------------------------------------------

def _positive(raw: str) -> int:
    n = int(raw)
    if n < 1:
        raise argparse.ArgumentTypeError(f"must be at least 1, not {n}")
    return n


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="litmo", description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    def with_names(p):
        p.add_argument("artifacts", nargs="*",
                       help="which to move (default: everything in sync.toml)")
        return p

    p = with_names(sub.add_parser("pull", help="bucket -> here"))
    p.add_argument("--force", action="store_true",
                   help="download even if the local copy already matches")
    p.add_argument("--clean", action="store_true",
                   help="remove local files the bucket does not have")
    p.add_argument("-w", "--workers", type=_positive, default=8,
                   help="parallel downloads and local hashing "
                        "for the `mirror` kind")
    p.set_defaults(func=cmd_pull)

    p = with_names(sub.add_parser("push", help="here -> bucket"))
    p.add_argument("--force", action="store_true",
                   help="upload even if the remote copy already matches")
    p.add_argument("--dry-run", action="store_true",
                   help="report what would move, upload nothing")
    p.add_argument("-w", "--workers", type=_positive, default=8,
                   help="parallel uploads and local hashing "
                        "for the `mirror` kind")
    p.set_defaults(func=cmd_push)

    p = with_names(sub.add_parser(
        "status", help="compare local and remote; non-zero if they differ"))
    p.set_defaults(func=cmd_status)

    sub.add_parser("mk", help="print the shared Makefile"
                   ).set_defaults(func=cmd_mk)
    sub.add_parser("doctor", help="check the toolchain, config and credentials"
                   ).set_defaults(func=cmd_doctor)

    args = ap.parse_args(argv)
    try:
        return args.func(args)
    except urllib.error.URLError as e:
        # A bucket that is not answering is a thing that happens on a train,
        # and it should read like one rather than like a bug in litmo.
        raise SystemExit(f"  could not reach the bucket: "
                         f"{getattr(e, 'reason', e)}") from None


if __name__ == "__main__":
    sys.exit(main())
