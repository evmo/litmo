"""Tests for the parts where being wrong costs real data.

The manifest migration matters most: three buckets already exist, written by
two earlier tools, and litmo has to read them without a re-upload. After that
come the two properties everything else rests on — that nothing from the
bucket lands outside the checkout, and that a pull which fails verification
leaves the previous local copy alone.

    uv run python -m unittest discover -s tests
"""

from __future__ import annotations

import contextlib
import dataclasses
import datetime
import email.message
import email.utils
import errno
import hashlib
import http.client
import io
import json
import os
import shutil
import socket
import subprocess
import tarfile
import tempfile
import threading
import time
import unittest
import unittest.mock
import urllib.error
import warnings
from pathlib import Path

from litmo import cli, config, creds, kinds, paths
from litmo import remote as transport
from litmo.hashing import file_sha256, human, tree_hash
from litmo.manifest import Malformed, Manifest
from litmo.remote import Conflict, Missing, Oversized, Remote, _url

SYNC_TOML = """\
[remote]
base = "https://example.invalid/mirror"

[artifact.cache]
kind = "archive"
path = "data/cache"
key  = "v1/data-cache.tar.zst"
what = "raw API responses"

[artifact.out]
kind = "mirror"
path = "out"
include = [".csv", ".json"]
"""

# Stand-ins that are the right *shape*: the manifest reader rejects a digest
# that is not 64 hex characters, so fixtures cannot use "aa" any more.
H1, H2, H3 = "1" * 64, "2" * 64, "3" * 64


def _other_filesystem():
    """A writable directory on a different filesystem from the temporary one,
    for the tests that need a real EXDEV rather than an injected one."""
    here = os.stat(tempfile.gettempdir()).st_dev
    for cand in ("/dev/shm", f"/run/user/{os.getuid()}",
                 os.path.expanduser("~")):
        try:
            if os.stat(cand).st_dev != here and os.access(cand, os.W_OK):
                return cand
        except OSError:
            continue
    return None


OTHER_FS = _other_filesystem()


def _gnu_make() -> bool:
    """`make -j` ordering is only meaningful against a make that has -j and
    the recursive-line rule — every GNU one does, BSD's does not."""
    try:
        out = subprocess.run(["make", "--version"], capture_output=True,
                             text=True, timeout=30).stdout
    except (OSError, subprocess.SubprocessError):
        return False
    return "GNU Make" in out


GNU_MAKE = _gnu_make()


class Fake:
    """An in-memory bucket that behaves like litmo.remote.Remote."""

    def __init__(self):
        self.objects: dict[str, bytes] = {}
        self.etags: dict[str, str] = {}
        self.public = True
        self.where = "memory://test"
        self.fail_upload_after = None      # nth upload raises
        self.uploads = 0
        self._seq = 0

    def _stamp(self, key):
        self._seq += 1
        self.etags[key] = f"etag-{self._seq}"

    def get_bytes(self, key, *, limit=None):
        if key not in self.objects:
            raise Missing(key)
        body = self.objects[key]
        if limit is not None and len(body) > limit:
            raise Oversized(key)
        return body, self.etags.get(key)

    def download(self, key, dest, max_bytes=None):
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(self.objects[key])

    def download_many(self, jobs, workers=8, label=""):
        for job in jobs:
            self.download(*job)

    def upload(self, src, key, content_type):
        self.uploads += 1
        if self.fail_upload_after and self.uploads > self.fail_upload_after:
            raise OSError("connection reset")
        self.objects[key] = Path(src).read_bytes()
        self._stamp(key)

    def put_bytes(self, key, body, content_type, *, if_match=None,
                  if_absent=False):
        if if_match is not None and self.etags.get(key) != if_match:
            raise Conflict(f"{key} moved under us")
        if if_absent and key in self.objects:
            raise Conflict(f"{key} appeared under us")
        self.objects[key] = body
        self._stamp(key)


class Ctx:
    def __init__(self, cfg, remote, manifest):
        self.cfg, self.remote, self.manifest = cfg, remote, manifest
        self.state = {}


class Base(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp(prefix="litmo-test-")).resolve()
        self.addCleanup(shutil.rmtree, self.root, ignore_errors=True)
        (self.root / "sync.toml").write_text(SYNC_TOML)
        self.cfg = config.load(self.root)

    def write(self, rel: str, text: str) -> Path:
        p = self.root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text)
        return p

    def reload(self, toml: str):
        (self.root / "sync.toml").write_text(toml)
        return config.load(self.root)


# --- configuration ----------------------------------------------------------

class TestConfig(Base):
    def test_parses_both_kinds(self):
        names = {a.name: a.kind for a in self.cfg.artifacts}
        self.assertEqual(names, {"cache": "archive", "out": "mirror"})
        self.assertEqual(self.cfg.base, "https://example.invalid/mirror")

    def test_public_schemas_name_the_behavior_changing_optional_keys(self):
        readme = Path(__file__).resolve().parents[1].joinpath("README.md").read_text()
        for docs in (config.__doc__, readme):
            docs = docs.replace("`litmo pull`", "`pull`")
            docs = docs.replace("`litmo push`", "`push`")
            docs = docs.replace("`litmo status`", "`status`")
            docs = " ".join(docs.split())
            self.assertIn('manifest = "manifest.json"', docs)
            self.assertIn("manual = true", docs)
            self.assertIn("bare `pull` or `push`", docs)
            self.assertRegex(docs, r"`status` (still|always) reports")

    def test_select_rejects_unknown(self):
        self.assertEqual([a.name for a in self.cfg.select(["out"])], ["out"])
        with self.assertRaises(SystemExit):
            self.cfg.select(["nope"])

    def test_include_must_be_a_list_of_dotted_suffixes(self):
        # Unvalidated, `include = ".csv"` was the tuple of its characters and
        # `["csv"]` a suffix no file has. Either matches nothing, the local
        # index comes back empty, and the next push drops every published
        # file from the manifest.
        for bad in ('".csv"', '["csv"]', '5', '[".csv", "json"]',
                    '[".csv", 3]', '["."]'):
            with self.assertRaises(SystemExit, msg=bad) as e:
                self.reload(SYNC_TOML.replace('include = [".csv", ".json"]',
                                              f"include = {bad}"))
            self.assertIn("include", str(e.exception))

    def test_include_is_refused_on_a_kind_that_never_reads_it(self):
        # Only `_walk` reads `include`, and only a mirror walks. Accepted
        # elsewhere it reads as a filter that is quietly doing nothing.
        with self.assertRaises(SystemExit) as e:
            self.reload(SYNC_TOML.replace(
                'key  = "v1/data-cache.tar.zst"',
                'key  = "v1/data-cache.tar.zst"\ninclude = [".json"]'))
        self.assertIn("include", str(e.exception))
        self.assertIn("archive", str(e.exception))

    def test_an_empty_include_still_means_every_file(self):
        cfg = self.reload(SYNC_TOML.replace('include = [".csv", ".json"]',
                                            "include = []"))
        art = {a.name: a for a in cfg.artifacts}["out"]
        self.assertEqual(art.include, ())
        self.write("out/anything.txt", "x")
        self.assertEqual([p.name for p in kinds._walk(self.root, art)],
                         ["anything.txt"])

    def test_manual_artifacts_are_skipped_by_a_bare_select(self):
        cfg = self.reload(
            SYNC_TOML +
            '\n[artifact.stages]\nkind = "fetch"\nmanual = true\n'
            'url = "https://example.invalid/stages.psv"\n'
            'path = "sources/stages.psv"\n')
        self.assertNotIn("stages", [a.name for a in cfg.select(None)])
        # ...but named explicitly, or asked for by status, it is there
        self.assertEqual([a.name for a in cfg.select(["stages"])], ["stages"])
        self.assertIn("stages",
                      [a.name for a in cfg.select(None, include_manual=True)])

    def test_manual_must_be_a_toml_boolean_not_a_truthy_value(self):
        for bad in ('"false"', '"true"', "1", "[]", "{}"):
            with self.subTest(bad=bad), self.assertRaises(SystemExit) as e:
                self.reload(SYNC_TOML.replace(
                    '[artifact.out]\nkind = "mirror"',
                    f'[artifact.out]\nkind = "mirror"\nmanual = {bad}'))
            self.assertIn("manual must be true or false", str(e.exception))

    def test_archive_without_key_is_rejected(self):
        with self.assertRaises(SystemExit):
            self.reload('[artifact.x]\nkind = "archive"\npath = "data"\n')

    def test_unknown_kind_is_rejected(self):
        with self.assertRaises(SystemExit):
            self.reload('[artifact.x]\nkind = "rsync"\npath = "data"\n')

    def test_escaping_path_is_rejected(self):
        for bad in ("../elsewhere", "/etc", "out/../..", "."):
            with self.subTest(bad), self.assertRaises(SystemExit):
                self.reload(f'[artifact.x]\nkind = "mirror"\npath = "{bad}"\n')

    def test_escaping_archive_key_is_rejected(self):
        with self.assertRaises(SystemExit):
            self.reload('[artifact.x]\nkind = "archive"\npath = "data"\n'
                        'key = "../../../etc/passwd"\n')

    def test_overlapping_artifacts_are_rejected(self):
        with self.assertRaises(SystemExit) as e:
            self.reload(SYNC_TOML +
                        '\n[artifact.inner]\nkind = "fetch"\n'
                        'url = "https://example.invalid/x.csv"\n'
                        'path = "out/x.csv"\n')
        self.assertIn("overlap", str(e.exception))

    def test_two_archives_may_not_share_a_key(self):
        with self.assertRaises(SystemExit) as e:
            self.reload(SYNC_TOML +
                        '\n[artifact.other]\nkind = "archive"\n'
                        'path = "data/other"\nkey = "v1/data-cache.tar.zst"\n')
        self.assertIn("both publish to key", str(e.exception))

    def test_an_archive_key_may_not_land_in_a_mirrors_namespace(self):
        # A mirror uploads each file to its own repo-relative path, so `out`
        # claims every key under `out/`. An archive aimed at one of them made
        # both push and the manifest commit succeed while the bucket could
        # only ever hold one of the two digests the manifest then named.
        for bad in ("out/a.csv", "out", "out/deep/bundle.tar.zst"):
            with self.subTest(bad), self.assertRaises(SystemExit) as e:
                self.reload(SYNC_TOML.replace("v1/data-cache.tar.zst", bad))
            self.assertIn("inside what mirror 'out' publishes",
                          str(e.exception))

    def test_a_key_that_merely_starts_like_a_mirrors_path_is_fine(self):
        cfg = self.reload(SYNC_TOML.replace("v1/data-cache.tar.zst",
                                            "outer/bundle.tar.zst"))
        self.assertEqual({a.name: a.key for a in cfg.artifacts}["cache"],
                         "outer/bundle.tar.zst")

    def test_an_archive_key_may_not_be_the_manifests_own_object(self):
        with self.assertRaises(SystemExit) as e:
            self.reload(SYNC_TOML.replace("v1/data-cache.tar.zst",
                                          "manifest.json"))
        self.assertIn("the manifest's own object", str(e.exception))

    def test_the_manifest_key_may_not_land_in_a_mirrors_namespace(self):
        with self.assertRaises(SystemExit) as e:
            self.reload(SYNC_TOML.replace(
                'base = "https://example.invalid/mirror"',
                'base = "https://example.invalid/mirror"\n'
                'manifest = "out/manifest.json"'))
        self.assertIn("overwrite the manifest", str(e.exception))

    def test_fetch_url_must_be_http(self):
        with self.assertRaises(SystemExit):
            self.reload('[artifact.x]\nkind = "fetch"\npath = "data/x"\n'
                        'url = "file:///etc/passwd"\n')

    def test_manifest_key_must_be_relative(self):
        with self.assertRaises(SystemExit):
            self.reload('[remote]\nmanifest = "/etc/passwd"\n\n'
                        '[artifact.x]\nkind = "mirror"\npath = "out"\n')

    def _with_base(self, base):
        return self.reload(f'[remote]\nbase = "{base}"\n\n'
                           '[artifact.x]\nkind = "mirror"\npath = "out"\n')

    def test_base_must_be_https(self):
        """`base` is where a credential-free reader gets the artifacts *and*
        the digests that vouch for them, so cleartext hands both to anyone on
        the path. `urlopen` would also open file:// and ftp://."""
        for base in ("http://artifacts.example.org", "file:///tmp",
                     "ftp://artifacts.example.org", "javascript:alert(1)",
                     "artifacts.example.org"):
            with self.subTest(base=base):
                with self.assertRaises(SystemExit) as e:
                    self._with_base(base)
                self.assertIn("must be an https", str(e.exception))

    def test_an_https_base_is_accepted_and_keeps_its_trailing_slash_trimmed(self):
        cfg = self._with_base("https://artifacts.example.org/mirror/")
        self.assertEqual(cfg.base, "https://artifacts.example.org/mirror")

    def test_no_base_at_all_is_the_s3_configuration_and_still_loads(self):
        cfg = self.reload('[remote]\nbucket = "b"\n\n'
                          '[artifact.x]\nkind = "mirror"\npath = "out"\n')
        self.assertEqual(cfg.base, "")


# --- path containment -------------------------------------------------------

class TestPaths(Base):
    def test_relative_rejects_the_usual_suspects(self):
        for bad in ("../x", "/etc/passwd", "", ".", "..", "a/../b", "a//b",
                    "a/", "./a", "C:/x", "a\\b", "x\0y", "x" * 2000):
            with self.subTest(bad), self.assertRaises(paths.Unsafe):
                paths.relative(bad)

    def test_relative_rejects_control_characters(self):
        """A bucket name is printed back on every pull, so ESC and CR are as
        much a part of what the terminal does with it as the letters are."""
        for raw in ("out/\x1b[32mverified\x1b[0m.csv",     # colour
                    "out/\x1b[2K\rup to date.csv",          # erase and repaint
                    "out/a\nb.csv", "out/a\rb.csv", "out/a\x07b.csv",
                    "out/a\tb.csv", "out/a\x7fb.csv", "out/a\x9bb.csv"):
            with self.subTest(raw=raw):
                with self.assertRaises(paths.Unsafe) as e:
                    paths.relative(raw)
                self.assertIn("control character", str(e.exception))
        # …and the refusal itself does not print the bytes it is refusing.
        with self.assertRaises(paths.Unsafe) as e:
            paths.relative("out/\x1b[2K\rgotcha.csv")
        self.assertNotIn("\x1b", str(e.exception))

    def test_display_escapes_a_name_relative_never_saw(self):
        self.assertEqual(paths.display("a\x1b[2K\rb"), "a\\x1b[2K\\x0db")
        self.assertEqual(paths.display("out/ordinary.csv"), "out/ordinary.csv")

    def test_relative_accepts_an_ordinary_name(self):
        self.assertEqual(paths.relative("out/sub/a.csv").as_posix(),
                         "out/sub/a.csv")

    def test_resolve_under_rejects_a_symlinked_parent(self):
        outside = Path(tempfile.mkdtemp(prefix="litmo-outside-"))
        self.addCleanup(shutil.rmtree, outside, ignore_errors=True)
        (self.root / "out").symlink_to(outside)
        with self.assertRaises(paths.Unsafe):
            paths.resolve_under(self.root, "out/a.csv")

    def test_resolve_under_refuses_to_write_through_a_symlink(self):
        outside = Path(tempfile.mkdtemp(prefix="litmo-outside-"))
        self.addCleanup(shutil.rmtree, outside, ignore_errors=True)
        (self.root / "out").mkdir()
        (self.root / "out/a.csv").symlink_to(outside / "target.csv")
        with self.assertRaises(paths.Unsafe):
            paths.resolve_under(self.root, "out/a.csv")

    def test_resolve_under_allows_a_path_that_does_not_exist_yet(self):
        got = paths.resolve_under(self.root, "out/deep/a.csv")
        self.assertEqual(got, self.root / "out/deep/a.csv")

    def test_under_artifact_rejects_another_artifacts_path(self):
        art = {a.name: a for a in self.cfg.artifacts}["out"]
        with self.assertRaises(paths.Unsafe):
            paths.under_artifact(self.root, art, "data/cache/x.csv")

    def test_under_artifact_follows_a_symlinked_artifact_directory(self):
        """`out -> /mnt/scratch` is a thing people do, and it is allowed."""
        outside = Path(tempfile.mkdtemp(prefix="litmo-outside-")).resolve()
        self.addCleanup(shutil.rmtree, outside, ignore_errors=True)
        (self.root / "out").symlink_to(outside)
        art = {a.name: a for a in self.cfg.artifacts}["out"]
        self.assertEqual(paths.under_artifact(self.root, art, "out/a.csv"),
                         outside / "a.csv")

    def test_under_artifact_still_bounds_a_symlinked_directory(self):
        """...but the bucket's names may not then wander out of it either."""
        outside = Path(tempfile.mkdtemp(prefix="litmo-outside-")).resolve()
        self.addCleanup(shutil.rmtree, outside, ignore_errors=True)
        (self.root / "out").symlink_to(outside)
        (outside / "sub").symlink_to(outside.parent)
        art = {a.name: a for a in self.cfg.artifacts}["out"]
        with self.assertRaises(paths.Unsafe):
            paths.under_artifact(self.root, art, "out/sub/elsewhere.csv")


# --- credentials on disk ----------------------------------------------------

class TestCreds(Base):
    """`.r2` is a live write credential; the mode it sits at is part of it."""

    FULL = ("R2_ACCOUNT_ID=acct\nR2_ACCESS_KEY_ID=akid\n"
            "R2_SECRET_ACCESS_KEY=" + "b" * 64 + "\nR2_BUCKET_NAME=buck\n")

    def setUp(self):
        super().setUp()
        # The environment wins over the file, so a stray R2_* in the shell
        # running the tests would otherwise decide their outcome. patch.dict
        # puts back whatever was there when each test ends.
        self.enterContext(unittest.mock.patch.dict(os.environ, clear=False))
        for k in (*creds.KEYS, "R2_BUCKET"):
            os.environ.pop(k, None)

    def creds_file(self, text: str, mode: int) -> Path:
        p = self.root / creds.CREDS_NAME
        p.write_text(text)
        p.chmod(mode)
        return p

    def test_checkout_ships_an_ignored_example_matching_the_loader(self):
        checkout = Path(__file__).resolve().parents[1]
        self.assertEqual((checkout / ".r2.example").read_text(), creds.EXAMPLE)
        self.assertIn("/.r2", (checkout / ".gitignore").read_text().splitlines())

    def test_loads_a_private_file(self):
        self.creds_file(self.FULL, 0o600)
        self.assertEqual(creds.load(self.root)["R2_BUCKET_NAME"], "buck")

    def test_refuses_a_world_readable_secret(self):
        self.creds_file(self.FULL, 0o644)
        with self.assertRaises(SystemExit) as e:
            creds.load(self.root)
        self.assertIn("chmod 600", str(e.exception))

    def test_refuses_a_group_readable_secret(self):
        self.creds_file(self.FULL, 0o640)
        with self.assertRaises(SystemExit):
            creds.load(self.root)

    def test_refuses_a_group_writable_secret(self):
        """Not just readable: another user substituting the key is worse."""
        self.creds_file(self.FULL, 0o620)
        with self.assertRaises(SystemExit):
            creds.load(self.root)

    def test_a_blank_template_is_not_a_secret(self):
        """`.r2` naming the bucket while CI supplies the key is legitimate."""
        self.creds_file(creds.EXAMPLE + "R2_BUCKET_NAME=buck\n", 0o644)
        os.environ.update(R2_ACCOUNT_ID="acct", R2_ACCESS_KEY_ID="akid",
                          R2_SECRET_ACCESS_KEY="c" * 64)
        self.assertEqual(creds.load(self.root)["R2_BUCKET_NAME"], "buck")

    def test_an_env_supplied_key_does_not_excuse_the_file(self):
        """The exposure is the bytes on disk, not which copy litmo used."""
        self.creds_file(self.FULL, 0o644)
        os.environ["R2_SECRET_ACCESS_KEY"] = "d" * 64
        with self.assertRaises(SystemExit):
            creds.load(self.root)

    def test_no_file_at_all_still_reports_what_is_missing(self):
        with self.assertRaises(SystemExit) as e:
            creds.load(self.root)
        self.assertIn("missing R2 credentials", str(e.exception))


# --- identity ---------------------------------------------------------------

# A fixed tree and the digest it must always have. Every other hash test
# below establishes a *relative* property — same bytes agree, changed bytes
# differ — which a changed serialisation satisfies just as well: both sides
# of a freshly generated round trip agree on the new format while every hash
# already committed to a bucket stops matching. Replacing the NUL separator
# in `hashing.tree_hash` with `|` left the whole suite green, and would have
# made every published archive unpullable.
#
# So this one value is not computed by litmo. It was derived from the
# format `hashing.tree_hash` documents — for each file, sorted by its
# path relative to the directory: the path, a NUL, the lowercase hex
# sha-256 of its bytes, a newline — using coreutils alone:
#
#   $ printf 'alpha\n' > a.txt; mkdir sub; printf 'beta\n' > sub/b.txt
#   $ ha=$(sha256sum a.txt | cut -d' ' -f1)
#   $ hb=$(sha256sum sub/b.txt | cut -d' ' -f1)
#   $ { printf 'a.txt'; printf '\0'; printf %s "$ha"; printf '\n'
#       printf 'sub/b.txt'; printf '\0'; printf %s "$hb"
#       printf '\n'; } | sha256sum
#   a3338c328701eec0ab2aadb60025f8bc97d9a3009b823432554cc1f2aebb5435
#
# Do not regenerate these when the implementation changes. A change that
# moves them is a change that orphans every manifest already in a bucket,
# and the only safe way to make one is to bump a format marker and teach
# the reader both.
TREE_FIXTURE = {"a.txt": "alpha\n", "sub/b.txt": "beta\n"}
FIXTURE_HASH = "a3338c328701eec0ab2aadb60025f8bc97d9a3009b823432554cc1f2aebb5435"
FIXTURE_FILES, FIXTURE_BYTES = 2, 11
# The same file hashed as a whole artifact of its own: `_files` takes the
# parent as the base, so the name is still part of the digest.
LONE_FILE_HASH = "e194db447df1acfc07e94a18c65cb878450f324b326b86cb012eb418f5a0b0a8"


class TestTreeHash(Base):
    def test_same_bytes_same_hash_regardless_of_creation_order(self):
        self.write("a/one.txt", "1")
        self.write("a/two.txt", "2")
        first = tree_hash(self.root / "a")

        shutil.rmtree(self.root / "a")
        self.write("a/two.txt", "2")
        self.write("a/one.txt", "1")
        self.assertEqual(tree_hash(self.root / "a"), first)

    def test_content_change_changes_hash(self):
        self.write("a/one.txt", "1")
        before = tree_hash(self.root / "a")[0]
        self.write("a/one.txt", "2")
        self.assertNotEqual(tree_hash(self.root / "a")[0], before)

    def test_rename_changes_hash(self):
        self.write("a/one.txt", "1")
        before = tree_hash(self.root / "a")[0]
        (self.root / "a/one.txt").rename(self.root / "a/uno.txt")
        self.assertNotEqual(tree_hash(self.root / "a")[0], before)

    def test_absent_tree(self):
        self.assertEqual(tree_hash(self.root / "nothing"), ("", 0, 0))

    def test_a_fixed_tree_keeps_the_digest_already_in_the_buckets(self):
        for rel, text in TREE_FIXTURE.items():
            self.write(f"fixture/{rel}", text)
        self.assertEqual(
            tree_hash(self.root / "fixture"),
            (FIXTURE_HASH, FIXTURE_FILES, FIXTURE_BYTES))
        self.assertEqual(tree_hash(self.root / "fixture/a.txt"),
                         (LONE_FILE_HASH, 1, 6))


# --- manifest migration -----------------------------------------------------

class TestManifestMigration(Base):
    def test_reads_new_format_unchanged(self):
        raw = {"artifacts": {"out": {"kind": "mirror", "path": "out",
                                     "files": [{"path": "out/a.csv", "size": 1,
                                                "sha256": H1}]}}}
        m = Manifest._migrate(raw, self.cfg)
        self.assertEqual(m.get("out")["kind"], "mirror")
        self.assertEqual(len(m.mirror_files("out")), 1)

    def test_legacy_archive_entries_get_a_kind(self):
        """An older bucket: artifacts keyed by name, no `kind` field."""
        raw = {"artifacts": {"cache": {
            "key": "v1/data-cache.tar.zst", "path": "data/cache",
            "tree_hash": H1, "archive_sha256": H2,
            "archive_bytes": 10, "raw_bytes": 100, "files": 5}}}
        m = Manifest._migrate(raw, self.cfg)
        self.assertEqual(m.get("cache")["kind"], "archive")
        self.assertEqual(m.get("cache")["tree_hash"], H1)

    def test_legacy_flat_file_list_is_grouped_onto_artifacts(self):
        """An older bucket still: one flat `files` list, no artifacts at all."""
        raw = {"generated": "2026-01-01T00:00Z", "files": [
            {"path": "out/a.csv", "size": 1, "sha256": H1},
            {"path": "out/sub/b.json", "size": 2, "sha256": H2},
            {"path": "data/cache/c.bin", "size": 3, "sha256": H3},
        ]}
        m = Manifest._migrate(raw, self.cfg)
        self.assertEqual({e["path"] for e in m.mirror_files("out")},
                         {"out/a.csv", "out/sub/b.json"})
        self.assertEqual(m.get("out")["kind"], "mirror")
        # data/cache is declared as an archive, and a flat list is a
        # mirror-shaped record: filing one under it would manufacture an
        # entry of the wrong kind that every verb then raises on. The point
        # is still that no record is lost — it is kept as an orphan, and
        # dump() writes it back out.
        self.assertIsNone(m.get("cache"))
        self.assertEqual([e["path"] for e in m.orphans], ["data/cache/c.bin"])
        self.assertIn(b"data/cache/c.bin", m.dump())

    def test_a_renamed_artifact_path_still_loads(self):
        # The bucket's copy lives under `out`; sync.toml now says `results`.
        # Measuring containment against the config rejected the whole
        # document — including for the push that would have rewritten it.
        cfg = self.reload(SYNC_TOML.replace('path = "out"',
                                            'path = "results"'))
        self.assertEqual({a.name: a.path.as_posix() for a in cfg.artifacts}
                         ["out"], "results")
        raw = {"artifacts": {"out": {
            "kind": "mirror", "path": "out",
            "files": [{"path": "out/x.csv", "size": 1, "sha256": H1}]}}}
        m = Manifest._migrate(raw, cfg)
        self.assertEqual(len(m.mirror_files("out")), 1)

    def test_an_entry_still_may_not_stray_from_its_own_path(self):
        raw = {"artifacts": {"out": {
            "kind": "mirror", "path": "out",
            "files": [{"path": "elsewhere/x", "size": 1, "sha256": H1}]}}}
        with self.assertRaises(Malformed):
            Manifest._migrate(raw, self.cfg)

    def test_files_no_artifact_claims_are_kept_as_orphans(self):
        raw = {"files": [{"path": "elsewhere/x", "size": 1, "sha256": H1}]}
        m = Manifest._migrate(raw, self.cfg)
        self.assertEqual(len(m.orphans), 1)
        self.assertIn(b"elsewhere/x", m.dump())

    def test_orphans_survive_a_round_trip(self):
        """dump() writes them alongside `artifacts`; load must not drop them."""
        raw = {"files": [{"path": "elsewhere/x", "size": 1, "sha256": H1}]}
        once = Manifest._migrate(raw, self.cfg)
        once.set("out", {"kind": "mirror", "path": "out", "files": []})
        import json
        twice = Manifest._migrate(json.loads(once.dump()), self.cfg)
        self.assertEqual(len(twice.orphans), 1)

    def test_prefix_match_does_not_bleed_across_artifacts(self):
        """`out` must not claim `outtakes/`."""
        raw = {"files": [{"path": "outtakes/x", "size": 1, "sha256": H1}]}
        m = Manifest._migrate(raw, self.cfg)
        self.assertEqual(m.mirror_files("out"), [])
        self.assertEqual(len(m.orphans), 1)

    def test_missing_manifest_is_empty_not_an_error(self):
        m = Manifest.load(Fake(), self.cfg)
        self.assertTrue(m.empty)
        self.assertFalse(m.existed)


# --- manifest validation ----------------------------------------------------

class TestManifestValidation(Base):
    def mirror(self, *files):
        return {"artifacts": {"out": {"kind": "mirror", "path": "out",
                                      "files": list(files)}}}

    def bad(self, raw):
        with self.assertRaises(Malformed) as e:
            Manifest._migrate(raw, self.cfg)
        return str(e.exception)

    def test_traversal_in_a_file_path_is_refused(self):
        self.assertIn("..", self.bad(self.mirror(
            {"path": "out/../../escape", "size": 1, "sha256": H1})))

    def test_absolute_file_path_is_refused(self):
        self.bad(self.mirror({"path": "/etc/passwd", "size": 1, "sha256": H1}))

    def test_a_file_outside_its_artifact_is_refused(self):
        self.assertIn("not under out/", self.bad(self.mirror(
            {"path": "data/cache/x", "size": 1, "sha256": H1})))

    def test_duplicate_entries_are_refused(self):
        e = {"path": "out/a.csv", "size": 1, "sha256": H1}
        self.assertIn("listed twice", self.bad(self.mirror(e, dict(e))))

    def test_a_path_carrying_terminal_escapes_is_refused(self):
        # Display-only, but the display is how an operator learns whether the
        # pull worked: `\x1b[2K\r` erases litmo's own line and repaints it.
        msg = self.bad(self.mirror(
            {"path": "out/\x1b[2K\rup to date  verified.csv",
             "size": 1, "sha256": H1}))
        self.assertIn("control character", msg)
        self.assertNotIn("\x1b", msg)

    def test_a_digest_that_is_not_a_digest_is_refused(self):
        self.bad(self.mirror({"path": "out/a.csv", "size": 1, "sha256": "aa"}))

    def test_a_negative_size_is_refused(self):
        self.bad(self.mirror({"path": "out/a.csv", "size": -1, "sha256": H1}))

    def test_a_size_that_is_a_string_is_refused(self):
        self.bad(self.mirror({"path": "out/a.csv", "size": "1", "sha256": H1}))

    def test_an_unknown_kind_is_refused(self):
        self.bad({"artifacts": {"out": {"kind": "rsync", "path": "out"}}})

    def test_an_archive_without_a_key_is_refused(self):
        self.bad({"artifacts": {"cache": {"kind": "archive", "path": "data",
                                          "tree_hash": H1,
                                          "archive_sha256": H2}}})

    def test_a_non_object_document_is_refused(self):
        self.bad(["not", "a", "manifest"])

    def test_invalid_json_is_refused(self):
        remote = Fake()
        remote.objects[self.cfg.manifest_key] = b"{not json at all"
        with self.assertRaises(Malformed):
            Manifest.load(remote, self.cfg)

    def test_an_oversized_manifest_is_refused(self):
        remote = Fake()
        remote.objects[self.cfg.manifest_key] = b"{}" + b" " * (33 << 20)
        with self.assertRaises(Oversized):
            Manifest.load(remote, self.cfg)

    def test_setting_an_identical_entry_is_not_a_change(self):
        m = Manifest({})
        entry = {"kind": "mirror", "path": "out", "files": []}
        m.set("out", entry)
        self.assertTrue(m.dirty)
        m.dirty = False
        m.set("out", dict(entry))
        self.assertFalse(m.dirty)


# --- the mirror kind --------------------------------------------------------

class TestMirror(Base):
    def art(self):
        return {a.name: a for a in self.cfg.artifacts}["out"]

    def ctx(self, manifest=None, remote=None):
        return Ctx(self.cfg, remote or Fake(), manifest or Manifest({}))

    def test_include_filter_and_skips(self):
        self.write("out/keep.csv", "a")
        self.write("out/keep2.json", "b")
        self.write("out/skip.txt", "c")
        self.write("out/__pycache__/skip.csv", "d")
        self.write("out/partial.csv.part", "e")
        got = {p.name for p in kinds._walk(self.root, self.art())}
        self.assertEqual(got, {"keep.csv", "keep2.json"})

    def test_the_local_index_pairs_every_digest_with_its_own_file(self):
        # The index is built by a pool now, so the two things that can break
        # silently are the pairing — a digest attached to the wrong path
        # publishes a manifest every reader fails to verify — and the order,
        # which is what `_classify` hands to the `--clean` sweep and what
        # `mirror_push` uploads in. Sizes descend as the names ascend, so a
        # loop that collected results as they completed would come back
        # roughly reversed.
        want = {}
        for i in range(24):
            rel = f"out/d{i % 3}/f{i:02d}.csv"
            body = f"file {i} " + "x" * (2000 - 60 * i)
            self.write(rel, body)
            want[rel] = (len(body.encode()),
                         hashlib.sha256(body.encode()).hexdigest())

        art = self.art()
        walked = [p.relative_to(self.root).as_posix()
                  for p in kinds._walk(self.root, art)]
        idx, total = kinds._local_index(self.ctx(), art)

        self.assertEqual(list(idx), walked)
        self.assertEqual({rel: (e["size"], e["sha256"])
                          for rel, e in idx.items()}, want)
        self.assertEqual(total, sum(size for size, _ in want.values()))
        # Width is a performance knob and nothing else: one worker and eight
        # must return the same index in the same order.
        one, one_total = kinds._local_index(self.ctx(), art, workers=1)
        self.assertEqual(one, idx)
        self.assertEqual(list(one), list(idx))
        self.assertEqual(one_total, total)

    def test_an_unpublishable_name_is_refused_before_a_byte_is_read(self):
        # A control character is a legal POSIX filename and an illegal
        # manifest path. Every name is checked before the pool starts, so the
        # refusal is the same one whichever worker would have reached the
        # file first.
        self.write("out/fine.csv", "a")
        self.write("out/weird.csv", "b")

        def never(path):
            raise AssertionError(f"hashed {path} before checking the names")

        with unittest.mock.patch.object(kinds, "file_sha256", never):
            with self.assertRaises(SystemExit) as e:
                kinds._local_index(self.ctx(), self.art())
        self.assertIn("control character", str(e.exception))
        self.assertIn("before publishing", str(e.exception))

    def test_push_then_pull_round_trip(self):
        self.write("out/a.csv", "hello")
        self.write("out/b.json", "{}")
        remote, man = Fake(), Manifest({})
        ctx = self.ctx(man, remote)

        self.assertTrue(kinds.mirror_push(ctx, self.art()))
        self.assertEqual(set(remote.objects), {"out/a.csv", "out/b.json"})

        shutil.rmtree(self.root / "out")
        kinds.mirror_pull(ctx, self.art())
        self.assertEqual((self.root / "out/a.csv").read_text(), "hello")

    def test_push_is_a_no_op_when_nothing_changed(self):
        # The second call returning false only means something if the first
        # one actually published: with `mirror_push` gutted to `return False`
        # every assertion below the first used to still hold.
        self.write("out/a.csv", "hello")
        remote, man = Fake(), Manifest({})
        ctx = self.ctx(man, remote)
        self.assertTrue(kinds.mirror_push(ctx, self.art()))
        self.assertEqual(set(remote.objects), {"out/a.csv"})
        self.assertEqual(len(man.mirror_files("out")), 1)
        uploads = remote.uploads

        man.dirty = False
        self.assertFalse(kinds.mirror_push(ctx, self.art()))
        self.assertFalse(man.dirty)
        self.assertEqual(remote.uploads, uploads)

    def test_push_notices_a_deletion_even_with_nothing_to_upload(self):
        self.write("out/a.csv", "hello")
        self.write("out/b.csv", "there")
        remote, man = Fake(), Manifest({})
        ctx = self.ctx(man, remote)
        kinds.mirror_push(ctx, self.art())
        (self.root / "out/b.csv").unlink()
        self.assertTrue(kinds.mirror_push(ctx, self.art()))
        self.assertEqual(len(man.mirror_files("out")), 1)

    def test_deleting_the_last_file_publishes_an_empty_mirror(self):
        self.write("out/a.csv", "hello")
        remote, man = Fake(), Manifest({})
        ctx = self.ctx(man, remote)
        kinds.mirror_push(ctx, self.art())
        (self.root / "out/a.csv").unlink()
        self.assertTrue(kinds.mirror_push(ctx, self.art()))
        self.assertEqual(man.mirror_files("out"), [])

    def test_an_absent_directory_is_a_skip_not_an_empty_mirror(self):
        self.write("out/a.csv", "hello")
        remote, man = Fake(), Manifest({})
        ctx = self.ctx(man, remote)
        kinds.mirror_push(ctx, self.art())
        shutil.rmtree(self.root / "out")
        self.assertFalse(kinds.mirror_push(ctx, self.art()))
        self.assertEqual(len(man.mirror_files("out")), 1)

    def test_an_include_filter_matching_nothing_publishes_empty(self):
        self.write("out/a.csv", "hello")
        remote, man = Fake(), Manifest({})
        ctx = self.ctx(man, remote)
        kinds.mirror_push(ctx, self.art())
        (self.root / "out/a.csv").unlink()
        self.write("out/only.txt", "not included")
        self.assertTrue(kinds.mirror_push(ctx, self.art()))
        self.assertEqual(man.mirror_files("out"), [])

    def test_a_mirrored_file_past_the_ceiling_is_refused_before_the_transfer(self):
        # Same promise, same reason to distrust it: a mirror file's only bound
        # is the size the manifest gives it.
        self.write("out/a.csv", "x,y\n")
        remote, man = Fake(), Manifest({})
        ctx = Ctx(self.cfg, remote, man)
        kinds.mirror_push(ctx, self.art())
        entry = dict(man.get("out"))
        entry["files"] = [dict(f, size=10 ** 30) for f in entry["files"]]
        man.set("out", entry)
        (self.root / "out/a.csv").unlink()

        asked = []
        remote.download_many = lambda jobs, **kw: asked.extend(jobs)
        with self.assertRaises(SystemExit) as e:
            kinds.mirror_pull(ctx, self.art())
        self.assertIn("past the", str(e.exception))
        self.assertIn("nothing under out was changed", str(e.exception))
        self.assertEqual(asked, [])

    def test_pull_verifies_checksums(self):
        self.write("out/a.csv", "hello")
        remote, man = Fake(), Manifest({})
        ctx = self.ctx(man, remote)
        kinds.mirror_push(ctx, self.art())

        remote.objects["out/a.csv"] = b"tampered"
        (self.root / "out/a.csv").unlink()
        with self.assertRaises(SystemExit):
            kinds.mirror_pull(ctx, self.art())

    def test_verification_names_every_bad_file_in_manifest_order(self):
        # The staged tree is verified by a pool now. Two things that would
        # break quietly: stopping at the first failure, which turns "12 of 14
        # failed" into "1 of 14" and hides how bad the bucket is, and losing
        # the order, which makes the ten names it prints a different ten each
        # run. Twelve of fourteen are tampered with, so the message has to
        # elide as well.
        want = {f"out/f{i:02d}.csv": f"body {i}".encode() for i in range(14)}
        for rel, body in want.items():
            self.write(rel, body.decode())
        remote, man = Fake(), Manifest({})
        ctx = self.ctx(man, remote)
        kinds.mirror_push(ctx, self.art())

        shutil.rmtree(self.root / "out")
        good = {"out/f03.csv", "out/f11.csv"}
        for rel in want:
            if rel not in good:
                remote.objects[rel] = b"tampered " + rel.encode()

        with self.assertRaises(SystemExit) as e:
            kinds.mirror_pull(ctx, self.art())
        msg = str(e.exception)
        self.assertIn("12 of 14 file(s) failed verification", msg)
        named = [ln.strip() for ln in msg.splitlines() if ln.startswith("    o")]
        self.assertEqual(named, [r for r in sorted(want) if r not in good][:10])
        self.assertIn("and 2 more", msg)
        self.assertFalse((self.root / "out").exists())

    def test_a_failed_pull_leaves_the_local_copy_alone(self):
        self.write("out/a.csv", "good")
        self.write("out/b.csv", "also good")
        remote, man = Fake(), Manifest({})
        ctx = self.ctx(man, remote)
        kinds.mirror_push(ctx, self.art())

        # Both objects change; only one still matches its manifest digest.
        remote.objects["out/a.csv"] = b"new good"
        man.artifacts["out"]["files"][0]["sha256"] = file_sha256(
            self.write("out/scratch.tmp", "new good"))
        man.artifacts["out"]["files"][0]["size"] = len(b"new good")
        (self.root / "out/scratch.tmp").unlink()
        remote.objects["out/b.csv"] = b"corrupt"

        with self.assertRaises(SystemExit) as e:
            kinds.mirror_pull(ctx, self.art(), force=True)
        self.assertIn("nothing under out was changed", str(e.exception))
        self.assertEqual((self.root / "out/a.csv").read_text(), "good")
        self.assertEqual((self.root / "out/b.csv").read_text(), "also good")

    def test_pull_and_status_ignore_what_include_does_not_cover(self):
        # A legacy flat list, which the manifest reader takes transparently,
        # can name a file this artifact's `include` excludes. The local index
        # never sees it, so before this every pull downloaded it again and
        # every status called it missing again.
        body = b"<h1>report</h1>"
        remote, man = Fake(), Manifest({})
        remote.objects["out/a.csv"] = b"A"
        remote.objects["out/report.html"] = body
        remote.objects["manifest.json"] = json.dumps({"files": [
            {"path": "out/a.csv", "size": 1,
             "sha256": hashlib.sha256(b"A").hexdigest()},
            {"path": "out/report.html", "size": len(body),
             "sha256": hashlib.sha256(body).hexdigest()},
        ]}).encode()
        remote._stamp("manifest.json")

        man = Manifest.load(remote, self.cfg)
        self.assertEqual(len(man.mirror_files("out")), 2)   # both migrated
        ctx = self.ctx(man, remote)

        kinds.mirror_pull(ctx, self.art())
        self.assertTrue((self.root / "out/a.csv").exists())
        self.assertFalse((self.root / "out/report.html").exists())
        self.assertEqual(kinds.mirror_status(ctx, self.art()).verdict,
                         "in sync")

    def test_push_says_uncovered_rather_than_no_longer_here(self):
        self.write("out/a.csv", "A")
        self.write("out/notes.txt", "not covered by include")
        remote, man = Fake(), Manifest({})
        man.artifacts["out"] = {
            "kind": "mirror", "path": "out",
            "files": [{"path": "out/notes.txt", "size": 1, "sha256": H1}]}
        ctx = self.ctx(man, remote)

        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            kinds.mirror_push(ctx, self.art())
        self.assertIn("no longer covered by `include`", out.getvalue())
        self.assertNotIn("no longer here", out.getvalue())

    def test_pull_refuses_a_manifest_path_outside_the_artifact(self):
        remote, man = Fake(), Manifest({})
        man.artifacts["out"] = {
            "kind": "mirror", "path": "out",
            "files": [{"path": "out/../../escape", "size": 1, "sha256": H1}]}
        with self.assertRaises(SystemExit):
            kinds.mirror_pull(self.ctx(man, remote), self.art())
        self.assertFalse((self.root.parent / "escape").exists())

    def test_a_stale_path_loads_but_pull_refuses_and_push_mends_it(self):
        # What the load-time check used to buy, bought per write instead:
        # the entry is readable, so status and push work, and every path it
        # names is still measured against the config before anything lands.
        self.write("out/x.csv", "X")
        remote, man = Fake(), Manifest({})
        kinds.mirror_push(self.ctx(man, remote), self.art())

        (self.root / "results").mkdir()
        (self.root / "out/x.csv").rename(self.root / "results/x.csv")
        cfg = self.reload(SYNC_TOML.replace('path = "out"',
                                            'path = "results"'))
        art = {a.name: a for a in cfg.artifacts}["out"]
        self.assertEqual(art.path.as_posix(), "results")
        ctx = Ctx(cfg, remote, Manifest._migrate(
            json.loads(man.dump()), cfg))

        self.assertFalse(kinds.mirror_status(ctx, art).ok)
        with self.assertRaises(SystemExit) as e:
            kinds.mirror_pull(ctx, art)
        self.assertIn("is not under results/", str(e.exception))
        self.assertFalse((self.root / "out/x.csv").exists())   # wrote nothing

        self.assertTrue(kinds.mirror_push(ctx, art))
        self.assertEqual([f["path"] for f in ctx.manifest.mirror_files("out")],
                         ["results/x.csv"])

    def test_pull_into_a_symlinked_artifact_directory_stays_inside_it(self):
        outside = Path(tempfile.mkdtemp(prefix="litmo-outside-")).resolve()
        self.addCleanup(shutil.rmtree, outside, ignore_errors=True)
        self.write("out/a.csv", "hello")
        remote, man = Fake(), Manifest({})
        ctx = self.ctx(man, remote)
        kinds.mirror_push(ctx, self.art())

        shutil.rmtree(self.root / "out")
        (self.root / "out").symlink_to(outside)
        kinds.mirror_pull(ctx, self.art())
        self.assertEqual((outside / "a.csv").read_text(), "hello")

        # An escape through that directory is still an escape.
        (outside / "sub").symlink_to(outside.parent)
        man.artifacts["out"]["files"] = [
            {"path": "out/sub/escape.csv", "size": 1, "sha256": H1}]
        with self.assertRaises(SystemExit):
            kinds.mirror_pull(ctx, self.art())
        self.assertFalse((outside.parent / "escape.csv").exists())

    def test_push_reads_through_a_symlinked_artifact_directory(self):
        outside = Path(tempfile.mkdtemp(prefix="litmo-outside-")).resolve()
        self.addCleanup(shutil.rmtree, outside, ignore_errors=True)
        (outside / "a.csv").write_text("hello")
        (self.root / "out").symlink_to(outside)

        remote, man = Fake(), Manifest({})
        self.assertTrue(kinds.mirror_push(self.ctx(man, remote), self.art()))
        self.assertEqual(set(remote.objects), {"out/a.csv"})
        self.assertEqual(remote.objects["out/a.csv"], b"hello")

    def test_push_refuses_a_name_the_manifest_cannot_carry(self):
        self.write("out/we\\ird.csv", "x")
        with self.assertRaises(SystemExit) as e:
            kinds.mirror_push(self.ctx(), self.art())
        self.assertIn("backslash", str(e.exception))

    def test_an_interrupted_push_records_what_actually_landed(self):
        self.write("out/a.csv", "one")
        self.write("out/b.csv", "two")
        remote, man = Fake(), Manifest({})
        ctx = self.ctx(man, remote)
        kinds.mirror_push(ctx, self.art())          # both published

        self.write("out/a.csv", "one changed")
        self.write("out/b.csv", "two changed")
        remote.fail_upload_after = remote.uploads + 1
        with self.assertRaises(OSError):
            kinds.mirror_push(ctx, self.art())

        # The manifest now describes the bucket: whichever file was uploaded
        # carries its new digest, the other still carries its old one.
        listed = {e["path"]: e["sha256"] for e in man.mirror_files("out")}
        on_disk = {p: file_sha256(self.root / p) for p in listed}
        in_bucket = {p: __import__("hashlib").sha256(
            remote.objects[p]).hexdigest() for p in listed}
        self.assertEqual(listed, in_bucket)
        self.assertNotEqual(listed, on_disk)        # one upload did not happen

    def test_a_concurrent_push_that_fails_still_describes_the_bucket(self):
        # Uploads run eight wide now, so when one fails the others are still
        # in flight. Whatever the manifest ends up listing has to be what the
        # bucket actually holds — a file whose PUT landed while the main
        # thread was handling the failure may not be left carrying the digest
        # the bucket had *before* it landed.
        for i in range(6):
            self.write(f"out/f{i}.csv", f"v1-{i}")
        remote, man = Fake(), Manifest({})
        ctx = self.ctx(man, remote)
        kinds.mirror_push(ctx, self.art())              # v1 published
        v1 = {e["path"]: e["sha256"] for e in man.mirror_files("out")}
        self.assertEqual(len(v1), 6)

        for i in range(6):
            self.write(f"out/f{i}.csv", f"v2-{i}")
        real_upload = remote.upload
        lock = threading.Lock()
        # Every worker has to be past the queue before the failure propagates.
        # `mirror_push` shuts its pool down with `cancel_futures=True`, so a
        # thread the scheduler has not got to yet is cancelled rather than
        # uploaded — right for a job still queued, but it would make the count
        # below a race on how fast the main thread reaches the failure. The
        # barrier makes "in flight" mean it; the timeout turns a pool narrower
        # than six into a failure rather than a hang.
        in_flight = threading.Barrier(6, timeout=10)

        def upload(src, key, content_type):
            in_flight.wait()
            if key.endswith("f0.csv"):
                raise OSError("connection reset")   # fails first, at once
            time.sleep(0.05)                        # the rest are still going
            with lock:
                real_upload(src, key, content_type)

        remote.upload = upload
        with self.assertRaises(OSError):
            kinds.mirror_push(ctx, self.art())

        listed = {e["path"]: e["sha256"] for e in man.mirror_files("out")}
        in_bucket = {p: hashlib.sha256(remote.objects[p]).hexdigest()
                     for p in listed}
        # Nothing is described by a digest the bucket does not hold …
        self.assertEqual(listed, in_bucket)
        # … and the five that did land are recorded, not thrown away.
        landed = [p for p in listed if listed[p] != v1[p]]
        self.assertEqual(len(landed), 5, listed)

    def test_a_rewrite_undone_before_the_re_read_is_still_refused(self):
        # `upload_file` reads the source in parts, and re-reads a part it
        # retries, so a writer working in place puts different generations
        # into different parts of one object. Re-reading the file afterwards
        # cannot see that if the writer put the original bytes back: the push
        # published the digest of sixteen A while the object held
        # AAAAAAAABBBBBBBB, and said nothing.
        f = self.write("out/x.csv", "A" * 16)
        read_half, mutated, uploaded, restored = (threading.Event()
                                                  for _ in range(4))

        class Chunked(Fake):
            def upload(self, src, key, content_type):
                self.uploads += 1
                with open(src, "rb", buffering=0) as fh:   # parts, separately
                    first = fh.read(8)
                    read_half.set()
                    mutated.wait(10)
                    rest = fh.read()
                self.objects[key] = first + rest
                self._stamp(key)
                # A real upload spends most of its wall clock after the last
                # read of the source: parts in flight, then the complete call.
                uploaded.set()
                restored.wait(10)

        def writer():
            read_half.wait(10)
            with open(f, "r+b") as fh:          # in place, same size
                fh.seek(8)
                fh.write(b"B" * 8)
            mutated.set()
            uploaded.wait(10)
            with open(f, "r+b") as fh:          # ... and put back
                fh.seek(8)
                fh.write(b"A" * 8)
            restored.set()

        remote, man = Chunked(), Manifest({})
        ctx = self.ctx(man, remote)
        t = threading.Thread(target=writer)
        t.start()
        self.addCleanup(t.join)
        with self.assertRaises(SystemExit) as e:
            kinds.mirror_push(ctx, self.art())
        t.join(10)

        self.assertIn("rewritten while they were uploading", str(e.exception))
        self.assertIn("out/x.csv", str(e.exception))
        # The bytes on disk are back to what they were, so only the timestamp
        # could have told anyone. The object really did move …
        self.assertEqual(f.read_bytes(), b"A" * 16)
        self.assertEqual(remote.objects["out/x.csv"], b"A" * 8 + b"B" * 8)
        # … so the file is left out of the manifest rather than published
        # under a digest the bucket does not hold.
        self.assertEqual(man.mirror_files("out"), [])

    def test_a_put_that_lands_but_cannot_be_re_read_is_dropped(self):
        # The PUT returned, so the object has moved; if the re-read that
        # decides whether the bytes are describable then blows up, the file
        # has to be left out of the manifest rather than keep the digest the
        # bucket held before. This is why the worker records `sent` itself
        # instead of the main thread doing it from the result.
        self.write("out/a.csv", "v1")
        self.write("out/b.csv", "v1")
        remote, man = Fake(), Manifest({})
        ctx = self.ctx(man, remote)
        kinds.mirror_push(ctx, self.art())
        v1 = {e["path"]: e["sha256"] for e in man.mirror_files("out")}

        self.write("out/a.csv", "v2")
        self.write("out/b.csv", "v2")
        real_unchanged = kinds._unchanged

        def unchanged(path, e, stamp=None):
            if path.name == "a.csv":
                raise RuntimeError("stat blew up after the PUT")
            return real_unchanged(path, e, stamp)

        with unittest.mock.patch.object(kinds, "_unchanged", unchanged):
            with self.assertRaises(RuntimeError):
                kinds.mirror_push(ctx, self.art())

        listed = {e["path"]: e["sha256"] for e in man.mirror_files("out")}
        self.assertNotIn("out/a.csv", listed)           # not listed at all …
        self.assertNotEqual(                            # … and it did move
            v1["out/a.csv"],
            hashlib.sha256(remote.objects["out/a.csv"]).hexdigest())

    def test_push_takes_a_worker_count_like_pull(self):
        for i in range(4):
            self.write(f"out/f{i}.csv", str(i))
        remote, man = Fake(), Manifest({})
        seen = set()

        real_upload = remote.upload
        lock = threading.Lock()

        def upload(src, key, content_type):
            seen.add(threading.current_thread().name)
            time.sleep(0.02)
            with lock:
                real_upload(src, key, content_type)

        remote.upload = upload
        kinds.mirror_push(self.ctx(man, remote), self.art(), workers=1)
        self.assertEqual(len(seen), 1)                  # honoured, not ignored
        self.assertEqual(len(man.mirror_files("out")), 4)

    def test_a_file_rewritten_while_it_uploads_is_not_published(self):
        # The digest is taken before the upload; a file that moves in between
        # would otherwise be listed under a digest the object does not have.
        self.write("out/a.csv", "one")
        remote, man = Fake(), Manifest({})
        ctx = self.ctx(man, remote)

        real_upload = remote.upload

        def racing_upload(src, key, content_type):
            real_upload(src, key, content_type)
            self.write("out/a.csv", "one, rewritten mid-upload")

        remote.upload = racing_upload
        with self.assertRaises(SystemExit) as e:
            kinds.mirror_push(ctx, self.art())
        self.assertIn("rewritten while they were uploading", str(e.exception))
        self.assertEqual(man.mirror_files("out"), [])

        # …and the next push, with nothing moving underneath it, publishes it.
        remote.upload = real_upload
        self.assertTrue(kinds.mirror_push(ctx, self.art()))
        self.assertEqual(man.mirror_files("out")[0]["sha256"],
                         file_sha256(self.root / "out/a.csv"))

    def test_a_known_file_rewritten_mid_upload_is_dropped_not_stale(self):
        # The PUT landed, so the object holds bytes this push cannot name:
        # not the digest it hashed (the file moved under it) and not the one
        # the bucket had before. Listing either names a digest the bucket
        # does not hold, and every reader's pull then fails verification.
        self.write("out/x.csv", "v1")
        remote, man = Fake(), Manifest({})
        ctx = self.ctx(man, remote)
        kinds.mirror_push(ctx, self.art())              # v1 published
        v1 = man.mirror_files("out")[0]["sha256"]

        self.write("out/x.csv", "v2")
        real_upload = remote.upload

        def racing_upload(src, key, content_type):
            real_upload(src, key, content_type)         # the PUT lands: v2
            self.write("out/x.csv", "v3")               # the build carries on

        remote.upload = racing_upload
        with self.assertRaises(SystemExit):
            kinds.mirror_push(ctx, self.art())

        self.assertEqual(man.mirror_files("out"), [])
        self.assertNotEqual(v1, __import__("hashlib").sha256(
            remote.objects["out/x.csv"]).hexdigest())

        # The next quiet push republishes it, and a reader can pull again.
        remote.upload = real_upload
        self.assertTrue(kinds.mirror_push(ctx, self.art()))
        self.assertEqual(man.mirror_files("out")[0]["sha256"],
                         file_sha256(self.root / "out/x.csv"))

    def test_an_interrupt_between_the_put_and_the_recheck_drops_the_file(self):
        # Same hole, reached by ^C in the post-upload re-hash of a large file
        # rather than by a racing writer: the upload returned, so the object
        # moved, and nothing added it to the verified set.
        self.write("out/x.csv", "v1")
        remote, man = Fake(), Manifest({})
        ctx = self.ctx(man, remote)
        kinds.mirror_push(ctx, self.art())
        self.write("out/x.csv", "v2")

        def interrupted(path, e, stamp=None):
            raise KeyboardInterrupt("^C during the post-upload re-hash")

        with unittest.mock.patch.object(kinds, "_unchanged", interrupted):
            with self.assertRaises(KeyboardInterrupt):
                kinds.mirror_push(ctx, self.art())
        self.assertEqual(man.mirror_files("out"), [])

    def test_a_manifest_of_another_kind_is_refused_not_a_traceback(self):
        # The other direction: sync.toml says mirror, the entry is an
        # archive, and `files` is a count. Iterating it raised TypeError
        # from status, pull *and* push.
        self.write("out/a.csv", "A")
        man = Manifest({"out": {
            "kind": "archive", "path": "out", "key": "k", "tree_hash": H1,
            "archive_sha256": H2, "archive_bytes": 1, "raw_bytes": 1,
            "files": 3}})
        ctx = self.ctx(man, Fake())
        for verb in (kinds.mirror_status, kinds.mirror_pull):
            with self.assertRaises(SystemExit) as e:
                verb(ctx, self.art())
            self.assertIn("declares this a mirror", str(e.exception))

        self.assertTrue(kinds.mirror_push(ctx, self.art()))
        self.assertEqual(man.get("out")["kind"], "mirror")
        self.assertEqual(len(man.mirror_files("out")), 1)

    def test_status_reports_missing_stale_and_extra(self):
        self.write("out/a.csv", "hello")
        remote, man = Fake(), Manifest({})
        ctx = self.ctx(man, remote)
        kinds.mirror_push(ctx, self.art())
        self.assertEqual(kinds.mirror_status(ctx, self.art()).verdict, "in sync")

        self.write("out/a.csv", "changed")
        self.write("out/extra.csv", "x")
        r = kinds.mirror_status(ctx, self.art())
        self.assertIn("stale", r.verdict)
        self.assertIn("extra", r.verdict)
        self.assertFalse(r.ok)

    def empty_mirror(self):
        """A bucket that has published this artifact and then emptied it."""
        self.write("out/a.csv", "hello")
        remote, man = Fake(), Manifest({})
        ctx = self.ctx(man, remote)
        kinds.mirror_push(ctx, self.art())
        (self.root / "out/a.csv").unlink()
        kinds.mirror_push(ctx, self.art())
        self.assertEqual(man.mirror_files("out"), [])
        return ctx

    def test_an_emptied_mirror_with_local_files_differs_not_local_only(self):
        # "local only" says the bucket knows nothing about this artifact. It
        # knows exactly what it holds — nothing — and `pull --clean` will act
        # on that, so the reader has to be told they are extras.
        ctx = self.empty_mirror()
        self.write("out/a.csv", "stale")
        r = kinds.mirror_status(ctx, self.art())
        self.assertEqual(r.verdict, "DIFFERS (1 extra)")
        self.assertEqual(r.remote, "0 files")
        self.assertFalse(r.ok)

    def test_an_emptied_mirror_with_nothing_local_is_in_sync(self):
        ctx = self.empty_mirror()
        r = kinds.mirror_status(ctx, self.art())
        self.assertEqual(r.verdict, "in sync")
        self.assertTrue(r.ok)

    def test_an_unpublished_artifact_is_still_absent_or_local_only(self):
        # The half the entry check must not disturb: with no manifest entry
        # the bucket really has said nothing.
        ctx = self.ctx(Manifest({}), Fake())
        self.assertEqual(kinds.mirror_status(ctx, self.art()).verdict, "absent")
        self.write("out/a.csv", "mine")
        r = kinds.mirror_status(ctx, self.art())
        self.assertEqual(r.verdict, "local only")
        self.assertEqual(r.remote, "absent")

    def test_a_published_mirror_with_nothing_local_is_remote_only(self):
        self.write("out/a.csv", "hello")
        remote, man = Fake(), Manifest({})
        ctx = self.ctx(man, remote)
        kinds.mirror_push(ctx, self.art())
        (self.root / "out/a.csv").unlink()
        self.assertEqual(kinds.mirror_status(ctx, self.art()).verdict,
                         "remote only")

    def test_pull_clean_removes_local_extras(self):
        self.write("out/a.csv", "hello")
        remote, man = Fake(), Manifest({})
        ctx = self.ctx(man, remote)
        kinds.mirror_push(ctx, self.art())
        self.write("out/extra.csv", "x")
        kinds.mirror_pull(ctx, self.art(), clean=True)
        self.assertFalse((self.root / "out/extra.csv").exists())

    def test_a_clean_pull_that_fails_partway_keeps_the_local_extras(self):
        # The sweep used to run before the install loop, so a `_move` that
        # raised left the artifact holding a mixture of two generations *and*
        # the local-only files already deleted. Everything else a failed
        # install leaves behind is one whole generation or the other and is
        # recovered by re-running the pull; an extra is in no bucket, so it
        # is the one thing the re-run cannot bring back.
        for n in "abc":
            self.write(f"out/{n}.csv", f"PUBLISHED-{n}")
        remote, man = Fake(), Manifest({})
        ctx = self.ctx(man, remote)
        kinds.mirror_push(ctx, self.art())
        for n in "abc":
            self.write(f"out/{n}.csv", f"local-{n}")
        self.write("out/extra.csv", "in no bucket anywhere")

        real_move, moved = kinds._move, []

        def failing_move(src, dest):
            moved.append(dest.name)
            if len(moved) == 2:                 # mid-loop, not on the first
                raise OSError(errno.EACCES, "Permission denied")
            return real_move(src, dest)

        with unittest.mock.patch.object(kinds, "_move", failing_move):
            with self.assertRaises(PermissionError):
                kinds.mirror_pull(ctx, self.art(), clean=True)

        self.assertEqual((self.root / "out/extra.csv").read_text(),
                         "in no bucket anywhere")
        # Every published file is one whole generation or the other …
        for n in "abc":
            self.assertIn((self.root / f"out/{n}.csv").read_text(),
                          (f"local-{n}", f"PUBLISHED-{n}"))
        # … and re-running the pull finishes the switch and sweeps.
        kinds.mirror_pull(ctx, self.art(), clean=True)
        self.assertFalse((self.root / "out/extra.csv").exists())
        for n in "abc":
            self.assertEqual((self.root / f"out/{n}.csv").read_text(),
                             f"PUBLISHED-{n}")

    def test_pull_clean_applies_an_empty_published_mirror(self):
        # Publishing the deletion of every file is a state, not an absence:
        # `--clean` has to apply it, or a reader keeps files the publisher
        # deliberately withdrew and `pull` still reports success.
        self.write("out/a.csv", "hello")
        remote, man = Fake(), Manifest({})
        ctx = self.ctx(man, remote)
        kinds.mirror_push(ctx, self.art())
        (self.root / "out/a.csv").unlink()
        self.assertTrue(kinds.mirror_push(ctx, self.art()))
        self.assertEqual(man.mirror_files("out"), [])

        self.write("out/a.csv", "stale")
        kinds.mirror_pull(ctx, self.art(), clean=True)
        self.assertFalse((self.root / "out/a.csv").exists())

    def test_pull_without_clean_keeps_files_an_empty_mirror_dropped(self):
        self.write("out/a.csv", "hello")
        remote, man = Fake(), Manifest({})
        ctx = self.ctx(man, remote)
        kinds.mirror_push(ctx, self.art())
        (self.root / "out/a.csv").unlink()
        kinds.mirror_push(ctx, self.art())

        self.write("out/a.csv", "stale")
        kinds.mirror_pull(ctx, self.art())
        self.assertEqual((self.root / "out/a.csv").read_text(), "stale")

    def test_pull_clean_sweeps_nothing_for_an_artifact_the_manifest_omits(self):
        # An empty mirror and an unpublished one look identical to
        # `_remote_index`. Only the first is a claim about what the bucket
        # holds; sweeping on the second would delete the only copy there is.
        self.write("out/a.csv", "irreplaceable")
        ctx = self.ctx(Manifest({}), Fake())
        kinds.mirror_pull(ctx, self.art(), clean=True)
        self.assertEqual((self.root / "out/a.csv").read_text(), "irreplaceable")

    def test_pull_clean_sweeps_when_include_covers_nothing_published(self):
        self.write("out/a.csv", "hello")
        remote, man = Fake(), Manifest({})
        ctx = self.ctx(man, remote)
        kinds.mirror_push(ctx, self.art())
        self.assertEqual(len(man.mirror_files("out")), 1)

        # `include` narrows to a suffix nothing published matches. The bucket
        # still describes the artifact, so `--clean` still applies it.
        self.cfg = self.reload(SYNC_TOML.replace('[".csv", ".json"]',
                                                 '[".json"]'))
        ctx = self.ctx(man, remote)
        self.write("out/b.json", "{}")
        kinds.mirror_pull(ctx, self.art(), clean=True)
        self.assertFalse((self.root / "out/b.json").exists())
        self.assertEqual((self.root / "out/a.csv").read_text(), "hello")

    def test_the_up_to_date_count_is_what_the_sweep_left(self):
        # The count was taken from the pre-sweep index, so a `--clean` pull
        # that had just deleted a file still claimed to be holding it.
        self.write("out/a.csv", "hello")
        remote, man = Fake(), Manifest({})
        ctx = self.ctx(man, remote)
        kinds.mirror_push(ctx, self.art())
        self.write("out/extra.csv", "x")

        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            kinds.mirror_pull(ctx, self.art(), clean=True)
        self.assertIn("up to date  (1 files)", out.getvalue())
        self.assertFalse((self.root / "out/extra.csv").exists())

    def test_the_up_to_date_count_is_untouched_without_clean(self):
        self.write("out/a.csv", "hello")
        remote, man = Fake(), Manifest({})
        ctx = self.ctx(man, remote)
        kinds.mirror_push(ctx, self.art())
        self.write("out/extra.csv", "x")

        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            kinds.mirror_pull(ctx, self.art())
        self.assertIn("up to date  (2 files)", out.getvalue())

    def test_a_failed_clean_pull_deletes_no_local_extras(self):
        # --clean removes the only copy there is: artifact directories are
        # git-ignored and the file is by definition not in the bucket. So it
        # has to wait for the download to verify.
        self.write("out/a.csv", "hello")
        remote, man = Fake(), Manifest({})
        ctx = self.ctx(man, remote)
        kinds.mirror_push(ctx, self.art())
        self.write("out/extra.csv", "irreplaceable")
        remote.objects["out/a.csv"] = b"tampered"

        with self.assertRaises(SystemExit) as e:
            kinds.mirror_pull(ctx, self.art(), force=True, clean=True)
        self.assertIn("nothing under out was changed", str(e.exception))
        self.assertEqual((self.root / "out/extra.csv").read_text(),
                         "irreplaceable")
        self.assertEqual((self.root / "out/a.csv").read_text(), "hello")

    def test_a_published_file_where_a_directory_sits_here_is_refused(self):
        # The published layout changed shape under a reader who still holds
        # the old one. os.replace of a file onto a directory raises from
        # inside the install loop — past the point where the pull has said
        # the tree is safe to change, so the tree ends up neither copy.
        for rel in ("out/a.csv", "out/m.csv", "out/z.csv"):
            self.write(rel, "published")
        remote, man = Fake(), Manifest({})
        ctx = self.ctx(man, remote)
        kinds.mirror_push(ctx, self.art())

        (self.root / "out/m.csv").unlink()
        self.write("out/m.csv/inner.csv", "irreplaceable")
        self.write("out/a.csv", "local")

        for clean in (False, True):
            with self.assertRaises(SystemExit) as e:
                kinds.mirror_pull(ctx, self.art(), clean=clean)
            self.assertIn("out was not touched", str(e.exception))
            self.assertIn("out/m.csv", str(e.exception))
            # Nothing installed, and --clean swept nothing on the way out.
            self.assertEqual((self.root / "out/a.csv").read_text(), "local")
            self.assertEqual((self.root / "out/m.csv/inner.csv").read_text(),
                             "irreplaceable")

    def test_the_artifact_directory_itself_may_be_the_file_in_the_way(self):
        # The same conflict one level up: the bucket holds a tree under
        # `out`, this checkout still holds a single file called `out`. The
        # mirror kind has no whole-tree swap to fall back on, so the move
        # loop's mkdir raised after the download had verified.
        self.write("out/a.csv", "published")
        remote, man = Fake(), Manifest({})
        ctx = self.ctx(man, remote)
        kinds.mirror_push(ctx, self.art())

        shutil.rmtree(self.root / "out")
        (self.root / "out").write_text("out is a file here")

        with self.assertRaises(SystemExit) as e:
            kinds.mirror_pull(ctx, self.art())
        self.assertIn("out was not touched", str(e.exception))
        self.assertEqual((self.root / "out").read_text(), "out is a file here")

    def test_a_published_directory_where_a_file_sits_here_is_refused(self):
        # The other direction: mkdir(parents=True) of a parent that is a file
        # raises, from the same place.
        self.write("out/a.csv", "published")
        self.write("out/sub.csv/x.csv", "published")
        remote, man = Fake(), Manifest({})
        ctx = self.ctx(man, remote)
        kinds.mirror_push(ctx, self.art())

        shutil.rmtree(self.root / "out/sub.csv")
        self.write("out/sub.csv", "was a file")
        self.write("out/a.csv", "local")

        with self.assertRaises(SystemExit) as e:
            kinds.mirror_pull(ctx, self.art())
        self.assertIn("out was not touched", str(e.exception))
        self.assertEqual((self.root / "out/a.csv").read_text(), "local")
        self.assertEqual((self.root / "out/sub.csv").read_text(), "was a file")


# --- the archive kind -------------------------------------------------------

# The three link shapes a bundle can carry, and where each points: one
# naming a regular file, one naming a directory, one naming nothing.
PUBLISHED_LINKS = {"tofile": "sub/x.json", "alias": "sub",
                   "dangling": "future.json"}


class TestArchive(Base):
    def art(self):
        return {a.name: a for a in self.cfg.artifacts}["cache"]

    def test_push_then_pull_round_trip(self):
        for i in range(5):
            self.write(f"data/cache/{i}.json", f'{{"n": {i}}}')
        before = tree_hash(self.root / "data/cache")

        remote, man = Fake(), Manifest({})
        ctx = Ctx(self.cfg, remote, man)
        self.assertTrue(kinds.archive_push(ctx, self.art()))
        self.assertIn("v1/data-cache.tar.zst", remote.objects)
        self.assertEqual(man.get("cache")["files"], 5)

        shutil.rmtree(self.root / "data/cache")
        kinds.archive_pull(ctx, self.art())
        self.assertEqual(tree_hash(self.root / "data/cache"), before)

    def _bundle_with_a_stray(self, remote, name="evil.txt"):
        """Repack the published bundle with one extra file beside the
        artifact, and correct the manifest's archive digest to match.

        The tree hash still describes `data/cache` alone, so the only thing
        that can catch the extra file is the stray walk.
        """
        key = "v1/data-cache.tar.zst"
        dctx = kinds._zstd().ZstdDecompressor()
        buf = io.BytesIO()
        cctx = kinds._zstd().ZstdCompressor(level=1)
        with cctx.stream_writer(buf, closefd=False) as z:
            with tarfile.open(fileobj=z, mode="w|") as out:
                with dctx.stream_reader(io.BytesIO(remote.objects[key])) as r:
                    with tarfile.open(fileobj=r, mode="r|") as src:
                        for m in src:
                            out.addfile(m, src.extractfile(m)
                                        if m.isreg() else None)
                info = tarfile.TarInfo(name)
                info.size = 4
                out.addfile(info, io.BytesIO(b"boo\n"))
        remote.objects[key] = buf.getvalue()
        entry = dict(self.man_for_stray.get("cache"))
        entry["archive_sha256"] = hashlib.sha256(buf.getvalue()).hexdigest()
        entry["archive_bytes"] = len(buf.getvalue())
        self.man_for_stray.set("cache", entry)

    def test_a_strays_name_is_escaped_before_it_reaches_the_terminal(self):
        """The one name on the pull path `paths.relative` never sees.

        Manifest paths are refused for a control character on the way in;
        a bundle's *member* names are not — the tar is opened, walked and
        the stray reported. That report is the message worth forging, since
        it is how the operator learns the pull was refused.
        """
        self.write("data/cache/1.json", '{"n": 1}')
        remote, man = Fake(), Manifest({})
        self.man_for_stray = man
        ctx = Ctx(self.cfg, remote, man)
        kinds.archive_push(ctx, self.art())
        self._bundle_with_a_stray(remote, name="\x1b[2Kverified\r.txt")
        before = tree_hash(self.root / "data/cache")

        with self.assertRaises(SystemExit) as e:
            kinds.archive_pull(ctx, self.art(), force=True)
        msg = str(e.exception)
        self.assertIn("file(s) outside", msg)
        self.assertNotIn("\x1b", msg)
        self.assertIn("\\x1b[2Kverified\\x0d.txt", msg)
        self.assertEqual(tree_hash(self.root / "data/cache"), before)

    def test_a_bundle_holding_a_file_outside_the_artifact_is_refused(self):
        # The stray walk skips the artifact's own subtree, which cannot hold
        # a stray. It still has to catch one that lands beside it.
        self.write("data/cache/1.json", '{"n": 1}')
        remote, man = Fake(), Manifest({})
        self.man_for_stray = man
        ctx = Ctx(self.cfg, remote, man)
        kinds.archive_push(ctx, self.art())
        self._bundle_with_a_stray(remote)

        before = tree_hash(self.root / "data/cache")
        with self.assertRaises(SystemExit) as cm:
            with contextlib.redirect_stdout(io.StringIO()):
                kinds.archive_pull(ctx, self.art(), force=True)
        self.assertIn("1 file(s) outside data/cache", str(cm.exception))
        self.assertIn("evil.txt", str(cm.exception))
        self.assertEqual(tree_hash(self.root / "data/cache"), before)
        self.assertFalse((self.root / "evil.txt").exists())

    def test_a_fresh_pull_reports_the_tree_it_verified(self):
        # The closing line no longer re-reads a tree it just renamed into
        # place, so its counts have to come out the same as before.
        for i in range(5):
            self.write(f"data/cache/{i}.json", f'{{"n": {i}}}')
        digest, n, size = tree_hash(self.root / "data/cache")
        ctx = Ctx(self.cfg, Fake(), Manifest({}))
        kinds.archive_push(ctx, self.art())
        shutil.rmtree(self.root / "data/cache")

        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            kinds.archive_pull(ctx, self.art())
        self.assertIn("verified", out.getvalue())
        self.assertIn(f"({n:,} files, {human(size)})", out.getvalue())
        self.assertEqual(tree_hash(self.root / "data/cache"), (digest, n, size))

    def test_a_merge_pull_still_counts_the_local_extras_it_kept(self):
        # The merge is the one path where the destination holds more than the
        # tree that was verified, so it is the one that still re-reads.
        self.write("data/cache/1.json", '{"n": 1}')
        ctx = Ctx(self.cfg, Fake(), Manifest({}))
        kinds.archive_push(ctx, self.art())
        self.write("data/cache/mine.json", '{"n": 2}')
        _, n, size = tree_hash(self.root / "data/cache")

        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            kinds.archive_pull(ctx, self.art(), force=True)
        self.assertIn("local extras remain", out.getvalue())
        self.assertIn(f"({n:,} files, {human(size)})", out.getvalue())

    def test_a_clean_pull_reports_only_the_buckets_tree(self):
        self.write("data/cache/1.json", '{"n": 1}')
        ctx = Ctx(self.cfg, Fake(), Manifest({}))
        kinds.archive_push(ctx, self.art())
        digest, n, size = tree_hash(self.root / "data/cache")
        self.write("data/cache/mine.json", '{"n": 2}')

        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            kinds.archive_pull(ctx, self.art(), force=True, clean=True)
        self.assertIn("verified", out.getvalue())
        self.assertIn(f"({n:,} files, {human(size)})", out.getvalue())
        self.assertEqual(tree_hash(self.root / "data/cache")[0], digest)

    def test_packing_does_not_look_up_an_owner_name_per_file(self):
        # tarfile.gettarinfo ends every member with pwd.getpwuid and
        # grp.getgrgid. On a host whose nsswitch merges systemd's user
        # database into `group` that is a socket round trip each — 300 µs
        # against ~45 µs of real work — and it was 94 % of pack time on a
        # 20,000-file tree. Nothing reads the names back: `data` extraction
        # drops ownership and identity is the tree hash.
        for i in range(40):
            self.write(f"data/cache/{i}.json", "{}")
        seen = []
        real = tarfile.grp.getgrgid
        with unittest.mock.patch.object(
                tarfile.grp, "getgrgid",
                lambda gid: (seen.append(gid), real(gid))[1]):
            kinds.archive_push(Ctx(self.cfg, Fake(), Manifest({})), self.art())
        self.assertEqual(seen, [], f"{len(seen)} group lookups while packing")

    def test_packed_headers_carry_no_account_names(self):
        # A bundle that names the publisher's accounts is one whose bytes
        # depend on who ran the push.
        self.write("data/cache/1.json", "{}")
        remote = Fake()
        kinds.archive_push(Ctx(self.cfg, remote, Manifest({})), self.art())
        names = set()
        dctx = kinds._zstd().ZstdDecompressor()
        with dctx.stream_reader(
                io.BytesIO(remote.objects["v1/data-cache.tar.zst"])) as z:
            with tarfile.open(fileobj=z, mode="r|") as tar:
                for m in tar:
                    names |= {m.uname, m.gname}
        self.assertEqual(names, {""})

    def test_an_internal_symlink_survives_the_round_trip_as_a_link(self):
        # _Tar builds the header itself, so the link has to keep its type and
        # its target rather than arriving as a copy of the file.
        self.write("data/cache/1.json", '{"n": 1}')
        os.symlink("1.json", self.root / "data/cache/link.json")
        before = tree_hash(self.root / "data/cache")

        ctx = Ctx(self.cfg, Fake(), Manifest({}))
        kinds.archive_push(ctx, self.art())
        shutil.rmtree(self.root / "data/cache")
        kinds.archive_pull(ctx, self.art())

        link = self.root / "data/cache/link.json"
        self.assertTrue(link.is_symlink())
        self.assertEqual(os.readlink(link), "1.json")
        self.assertEqual(tree_hash(self.root / "data/cache"), before)

    def _mixed_link_tree(self):
        """A published tree holding all three link shapes a bundle can carry:
        one naming a regular file, one naming a directory, one naming
        nothing. Returns the ctx it was published through."""
        self.write("data/cache/sub/x.json", '{"n": 1}')
        os.symlink("sub/x.json", self.root / "data/cache/tofile")
        os.symlink("sub", self.root / "data/cache/alias")
        os.symlink("future.json", self.root / "data/cache/dangling")
        ctx = Ctx(self.cfg, Fake(), Manifest({}))
        kinds.archive_push(ctx, self.art())
        return ctx

    def _links(self):
        d = self.root / "data/cache"
        return {p.relative_to(d).as_posix(): os.readlink(p)
                for p in sorted(d.rglob("*")) if p.is_symlink()}

    def test_a_merge_installs_every_published_link(self):
        # The merge installer selected with `is_file()`, which *follows* the
        # link: a symlink to a directory and a dangling symlink both answered
        # False and were dropped, while one naming a regular file came
        # through. `tree_hash` selects the same way, so the digest agreed
        # afterwards and the pull reported a superset it had not installed.
        ctx = self._mixed_link_tree()
        shutil.rmtree(self.root / "data/cache")
        self.write("data/cache/mine.json", "MINE")

        kinds.archive_pull(ctx, self.art())          # an ordinary merge

        d = self.root / "data/cache"
        self.assertEqual((d / "mine.json").read_text(), "MINE")
        self.assertEqual((d / "sub/x.json").read_text(), '{"n": 1}')
        self.assertEqual(self._links(), PUBLISHED_LINKS)

    def test_a_clean_pull_installs_every_published_link_too(self):
        # The parity the merge was missing: `--clean` renames the whole
        # verified tree into place, so it has always carried all three.
        ctx = self._mixed_link_tree()
        shutil.rmtree(self.root / "data/cache")
        self.write("data/cache/mine.json", "MINE")

        kinds.archive_pull(ctx, self.art(), clean=True)

        self.assertFalse((self.root / "data/cache/mine.json").exists())
        self.assertEqual(self._links(), PUBLISHED_LINKS)

    def test_a_merge_refuses_a_published_link_a_local_directory_blocks(self):
        # A link now being installed is a path that can be in the way, and it
        # is the same conflict a published file meets: refuse before the move
        # loop starts rather than fail ENOTEMPTY halfway through it.
        ctx = self._mixed_link_tree()
        shutil.rmtree(self.root / "data/cache")
        self.write("data/cache/alias/local.json", "MINE")

        with self.assertRaises(SystemExit) as e:
            kinds.archive_pull(ctx, self.art())
        self.assertIn("data/cache/alias is a directory here", str(e.exception))
        self.assertEqual((self.root / "data/cache/alias/local.json"
                          ).read_text(), "MINE")

    def test_publication_records_the_frozen_tree_digest_and_pulls_it_back(self):
        # The other two places the serialisation is load-bearing: the value
        # `archive_push` writes into the manifest (litmo/kinds.py) and the
        # value `archive_pull` re-checks the staged tree against. Both are
        # held to the constant in TestTreeHash rather than to whatever the
        # implementation computes today, so a changed format is a failure
        # here and not a bucket full of archives no reader can pull.
        for rel, text in TREE_FIXTURE.items():
            self.write(f"data/cache/{rel}", text)
        remote, man = Fake(), Manifest({})
        ctx = Ctx(self.cfg, remote, man)
        kinds.archive_push(ctx, self.art())

        entry = man.get("cache")
        self.assertEqual(entry["tree_hash"], FIXTURE_HASH)
        self.assertEqual(entry["files"], FIXTURE_FILES)
        self.assertEqual(entry["raw_bytes"], FIXTURE_BYTES)

        shutil.rmtree(self.root / "data/cache")
        kinds.archive_pull(ctx, self.art())
        self.assertEqual(self._tree(), TREE_FIXTURE)

    def test_a_hardlink_is_still_packed_as_a_link(self):
        # The fast header path skips anything with st_nlink > 1, so the
        # stdlib's inode bookkeeping still turns the second name into a
        # LNKTYPE member instead of a second copy of the bytes.
        self.write("data/cache/1.json", '{"n": 1}')
        os.link(self.root / "data/cache/1.json",
                self.root / "data/cache/same.json")
        remote = Fake()
        kinds.archive_push(Ctx(self.cfg, remote, Manifest({})), self.art())
        types = {}
        dctx = kinds._zstd().ZstdDecompressor()
        with dctx.stream_reader(
                io.BytesIO(remote.objects["v1/data-cache.tar.zst"])) as z:
            with tarfile.open(fileobj=z, mode="r|") as tar:
                for m in tar:
                    types[m.name] = m.type
        self.assertIn(tarfile.LNKTYPE, types.values())

    def test_pull_clean_removes_local_extras(self):
        self.write("data/cache/1.json", "{}")
        remote, man = Fake(), Manifest({})
        ctx = Ctx(self.cfg, remote, man)
        kinds.archive_push(ctx, self.art())
        self.write("data/cache/stale.json", "{}")
        kinds.archive_pull(ctx, self.art(), clean=True)
        self.assertFalse((self.root / "data/cache/stale.json").exists())
        self.assertTrue((self.root / "data/cache/1.json").exists())

    def test_pull_without_clean_merges_and_keeps_extras(self):
        # Both halves have to be *done*, not merely survive: the published
        # file has drifted locally and must be put back, another published
        # file is gone and must be installed, and the local-only file must
        # come through untouched. The earlier version of this test published
        # one file, created one extra and asserted both existed — which they
        # already did before the pull, so gutting `archive_pull` to `return
        # None` passed it.
        self.write("data/cache/1.json", '{"n": 1}')
        self.write("data/cache/2.json", '{"n": 2}')
        remote, man = Fake(), Manifest({})
        ctx = Ctx(self.cfg, remote, man)
        kinds.archive_push(ctx, self.art())

        self.write("data/cache/1.json", "local drift")   # to be replaced
        (self.root / "data/cache/2.json").unlink()       # to be restored
        self.write("data/cache/mine.json", "MINE")       # to be kept

        kinds.archive_pull(ctx, self.art())

        self.assertEqual(self._tree(), {"1.json": '{"n": 1}',
                                        "2.json": '{"n": 2}',
                                        "mine.json": "MINE"})

    def test_packing_leans_on_nothing_deprecated(self):
        # `_Tar.gettarinfo` deliberately drops the back-reference the stdlib
        # plants on each member. 3.12 writes it and calls it "Not needed";
        # 3.13 renamed it, made the old name warn, and marked it for removal
        # in 3.16 — and the 3.12 floor has no slot for the new name, so the
        # only spelling that works everywhere is not to set it at all.
        for i in range(3):
            self.write(f"data/cache/{i}.json", f'{{"n": {i}}}')
        self.write("data/cache/sub/deep.json", "{}")
        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            self.assertTrue(kinds.archive_push(
                Ctx(self.cfg, Fake(), Manifest({})), self.art()))

    def test_push_is_a_no_op_when_the_tree_is_unchanged(self):
        # Same defect as the mirror's no-op test: with `archive_push` gutted
        # to `return False` this passed without a bundle ever existing.
        self.write("data/cache/1.json", "{}")
        remote, man = Fake(), Manifest({})
        ctx = Ctx(self.cfg, remote, man)
        self.assertTrue(kinds.archive_push(ctx, self.art()))
        entry = dict(man.get("cache"))
        self.assertIn(entry["key"], remote.objects)
        uploads = remote.uploads

        man.dirty = False
        self.assertFalse(kinds.archive_push(ctx, self.art()))
        self.assertFalse(man.dirty)
        self.assertEqual(remote.uploads, uploads)
        self.assertEqual(man.get("cache"), entry)

    RENAMED_TOML = SYNC_TOML.replace(
        'path = "data/cache"', 'path = "data/renamed"').replace(
        'key  = "v1/data-cache.tar.zst"', 'key  = "v2/data-renamed.tar.zst"')

    def test_a_renamed_archive_is_republished_without_force(self):
        # A tree hash is built from paths relative to the artifact directory,
        # so a rename does not move it — and a no-op decision that reads only
        # the digest leaves the manifest describing `data/cache` while
        # sync.toml says `data/renamed`. The bundle in the bucket still holds
        # `data/cache/...`, so every reader's pull refuses it, and no
        # ordinary push ever mends it.
        self.write("data/cache/x.json", '{"n": 1}')
        remote, man = Fake(), Manifest({})
        self.assertTrue(kinds.archive_push(Ctx(self.cfg, remote, man),
                                           self.art()))

        (self.root / "data/cache").rename(self.root / "data/renamed")
        cfg = self.reload(self.RENAMED_TOML)
        art = {a.name: a for a in cfg.artifacts}["cache"]
        ctx = Ctx(cfg, remote, man)

        self.assertTrue(kinds.archive_push(ctx, art))   # not "up to date"
        self.assertIn("v2/data-renamed.tar.zst", remote.objects)

        # What a reader actually gets: the committed document, re-read.
        fresh = Manifest._migrate(json.loads(man.dump()), cfg)
        entry = fresh.get("cache")
        self.assertEqual(entry["path"], "data/renamed")
        self.assertEqual(entry["key"], "v2/data-renamed.tar.zst")

        shutil.rmtree(self.root / "data/renamed")
        kinds.archive_pull(Ctx(cfg, remote, fresh), art)
        self.assertEqual((self.root / "data/renamed/x.json").read_text(),
                         '{"n": 1}')

    def test_status_says_differs_while_the_bucket_holds_the_old_layout(self):
        # The same blind spot one verb over: `archive_status` compared only
        # the tree hash, so after a rename it said "in sync" and exited zero
        # — sending a maintainer away without the push that mends the bucket,
        # while a fresh reader's pull under the same config cannot work.
        self.write("data/cache/x.json", '{"n": 1}')
        remote, man = Fake(), Manifest({})
        kinds.archive_push(Ctx(self.cfg, remote, man), self.art())

        (self.root / "data/cache").rename(self.root / "data/renamed")
        cfg = self.reload(self.RENAMED_TOML)
        art = {a.name: a for a in cfg.artifacts}["cache"]
        ctx = Ctx(cfg, remote, man)

        report = kinds.archive_status(ctx, art)
        self.assertEqual(report.verdict, kinds.DIFFERS)
        self.assertFalse(report.ok)
        self.assertIn("elsewhere", report.remote)

        kinds.archive_push(ctx, art)
        self.assertEqual(kinds.archive_status(ctx, art).verdict, kinds.IN_SYNC)

    def test_a_rekeyed_archive_is_republished_without_force(self):
        # The key moves on its own too — a `v1/` to `v2/` bump with the tree
        # untouched. The manifest kept pointing at the old object.
        self.write("data/cache/x.json", '{"n": 1}')
        remote, man = Fake(), Manifest({})
        kinds.archive_push(Ctx(self.cfg, remote, man), self.art())

        cfg = self.reload(SYNC_TOML.replace('key  = "v1/data-cache.tar.zst"',
                                            'key  = "v2/data-cache.tar.zst"'))
        art = {a.name: a for a in cfg.artifacts}["cache"]
        self.assertTrue(kinds.archive_push(Ctx(cfg, remote, man), art))
        self.assertEqual(man.get("cache")["key"], "v2/data-cache.tar.zst")
        self.assertIn("v2/data-cache.tar.zst", remote.objects)

    def test_an_entry_that_never_recorded_a_path_is_not_re_uploaded(self):
        # A manifest written before entries carried `path` does not say where
        # it put itself, and a push must not read that silence as a move:
        # re-uploading every legacy archive to record something already true
        # is not what this decision is for.
        self.write("data/cache/x.json", '{"n": 1}')
        remote, man = Fake(), Manifest({})
        ctx = Ctx(self.cfg, remote, man)
        kinds.archive_push(ctx, self.art())
        legacy = {k: v for k, v in man.get("cache").items() if k != "path"}
        man.artifacts["cache"] = legacy
        man.dirty = False
        uploads = remote.uploads

        self.assertFalse(kinds.archive_push(ctx, self.art()))
        self.assertEqual(remote.uploads, uploads)
        self.assertFalse(man.dirty)

    def test_pull_rejects_a_corrupt_bundle(self):
        self.write("data/cache/1.json", "{}")
        remote, man = Fake(), Manifest({})
        ctx = Ctx(self.cfg, remote, man)
        kinds.archive_push(ctx, self.art())

        remote.objects["v1/data-cache.tar.zst"] = b"not a zstd stream"
        shutil.rmtree(self.root / "data/cache")
        with self.assertRaises(SystemExit):
            kinds.archive_pull(ctx, self.art())

    def test_a_corrupt_bundle_leaves_the_local_tree_alone(self):
        self.write("data/cache/1.json", '{"local": true}')
        remote, man = Fake(), Manifest({})
        ctx = Ctx(self.cfg, remote, man)
        kinds.archive_push(ctx, self.art())
        before = tree_hash(self.root / "data/cache")

        remote.objects["v1/data-cache.tar.zst"] = b"not a zstd stream"
        man.artifacts["cache"]["tree_hash"] = H1      # force a re-pull
        with self.assertRaises(SystemExit) as e:
            kinds.archive_pull(ctx, self.art())
        self.assertIn("was not touched", str(e.exception))
        self.assertEqual(tree_hash(self.root / "data/cache"), before)

    def test_a_bundle_that_is_not_the_manifests_tree_is_refused(self):
        self.write("data/cache/1.json", "{}")
        remote, man = Fake(), Manifest({})
        ctx = Ctx(self.cfg, remote, man)
        kinds.archive_push(ctx, self.art())
        before = tree_hash(self.root / "data/cache")

        # A bundle that verifies against archive_sha256 but holds other bytes.
        self.write("data/cache/1.json", '{"different": true}')
        other = Fake()
        kinds.archive_push(Ctx(self.cfg, other, Manifest({})), self.art())
        bundle = other.objects["v1/data-cache.tar.zst"]
        remote.objects["v1/data-cache.tar.zst"] = bundle
        man.artifacts["cache"]["archive_sha256"] = __import__(
            "hashlib").sha256(bundle).hexdigest()
        self.write("data/cache/1.json", "{}")         # back to the first tree

        with self.assertRaises(SystemExit) as e:
            kinds.archive_pull(ctx, self.art(), force=True)
        self.assertIn("not the tree the manifest describes", str(e.exception))
        self.assertEqual(tree_hash(self.root / "data/cache"), before)

    def test_a_bundle_that_will_not_unpack_is_a_clean_failure(self):
        self.write("data/cache/1.json", "{}")
        remote, man = Fake(), Manifest({})
        ctx = Ctx(self.cfg, remote, man)
        kinds.archive_push(ctx, self.art())
        before = tree_hash(self.root / "data/cache")

        # zstd-valid, tar-nonsense: past the digest check, into the unpack.
        import zstandard
        body = zstandard.ZstdCompressor().compress(b"this is not a tar stream")
        remote.objects["v1/data-cache.tar.zst"] = body
        man.artifacts["cache"]["archive_sha256"] = __import__(
            "hashlib").sha256(body).hexdigest()
        man.artifacts["cache"]["archive_bytes"] = len(body)

        with self.assertRaises(SystemExit) as e:
            kinds.archive_pull(ctx, self.art(), force=True)
        self.assertIn("was not touched", str(e.exception))
        self.assertEqual(tree_hash(self.root / "data/cache"), before)

    def _crafted_bundle(self, dest: Path, members) -> Path:
        """A real tar.zst holding exactly `members` — (TarInfo, bytes|None).

        Written member by member rather than by packing a directory, because
        the shapes it exists to carry are ones `_pack` refuses to create:
        nothing on the publishing side can produce a name with `..` in it.
        A hostile or corrupt bucket can serve one, and `_unpack` is the only
        thing between that bundle and the filesystem.
        """
        cctx = kinds._zstd().ZstdCompressor(level=1)
        with dest.open("wb") as raw, cctx.stream_writer(raw) as z:
            with tarfile.open(fileobj=z, mode="w|") as tar:
                for info, body in members:
                    tar.addfile(info, io.BytesIO(body) if body else None)
        return dest

    def test_unpack_refuses_members_that_write_outside_the_staging_tree(self):
        # The security boundary is `filter="data"` at the single `tar.extract`
        # in `_unpack`. Everything else on the pull path looks at the bundle
        # *after* extraction — the stray walk, the tree hash — and none of it
        # can undo a write that has already landed somewhere else. So this
        # holds the extraction itself: each member is refused, and the file it
        # aimed at still has its own bytes afterwards.
        stage = self.root / "stage"
        stage.mkdir()
        beside = stage / "sentinel"
        elsewhere = self.root / "elsewhere.txt"

        climbing = tarfile.TarInfo("../sentinel")
        climbing.size = 7
        leaving = tarfile.TarInfo("escape")
        leaving.type, leaving.linkname = tarfile.SYMTYPE, "../sentinel"
        device = tarfile.TarInfo("dev")
        device.type, device.devmajor, device.devminor = tarfile.CHRTYPE, 1, 3
        absolute = tarfile.TarInfo("absolute")
        absolute.name, absolute.size = str(elsewhere), 7

        for name, member, refused in (
                ("a member climbing out with ..", climbing, True),
                ("a symlink out of the tree", leaving, True),
                ("a device node", device, True),
                # Not refused — the `data` filter makes an absolute name
                # relative and extracts it *inside* the destination, which
                # is the same guarantee by another route. `fully_trusted`
                # joins it and writes straight to the path it names.
                ("an absolute member name", absolute, False)):
            with self.subTest(name):
                tree = stage / "tree"
                shutil.rmtree(tree, ignore_errors=True)
                tree.mkdir()
                beside.write_text("KEEP")
                elsewhere.write_text("KEEP")
                bundle = self._crafted_bundle(
                    stage / "hostile.tar.zst",
                    [(member, b"CHANGED" if member.isreg() else None)])
                if refused:
                    with self.assertRaises(tarfile.FilterError):
                        kinds._unpack(bundle, tree)
                    self.assertEqual(list(tree.iterdir()), [])
                else:
                    kinds._unpack(bundle, tree)
                    landed = [p for p in tree.rglob("*") if p.is_file()]
                    self.assertEqual([p.read_text() for p in landed],
                                     ["CHANGED"])
                self.assertEqual(beside.read_text(), "KEEP")
                self.assertEqual(elsewhere.read_text(), "KEEP")

    def test_the_expansion_limits_bite_before_the_member_that_crosses_them(self):
        # `MAX_MEMBERS` and `MAX_UNPACKED` are the only bound on what a
        # bundle expands to — the download is held to its *compressed* size,
        # and the digest that proves the bundle genuine has already matched
        # by the time `_unpack` runs. Disabling either branch left the whole
        # suite green, so both are pinned here at the boundary: two members
        # of four bytes each, with the limits moved down around them.
        stage = self.root / "stage"
        stage.mkdir()
        members = []
        for name in ("a.json", "b.json"):
            info = tarfile.TarInfo(f"data/cache/{name}")
            info.size = 4
            members.append((info, b"{ }\n"))
        bundle = self._crafted_bundle(stage / "two.tar.zst", members)

        for limits, refusal in (
                ({"MAX_MEMBERS": 2, "MAX_UNPACKED": 8}, None),
                ({"MAX_MEMBERS": 1, "MAX_UNPACKED": 8},
                 "holds more than 1 members"),
                ({"MAX_MEMBERS": 2, "MAX_UNPACKED": 7},
                 "unpacks to more than 7 B")):
            with self.subTest(**limits):
                tree = stage / "tree"
                shutil.rmtree(tree, ignore_errors=True)
                tree.mkdir()
                with unittest.mock.patch.multiple(kinds, **limits):
                    if refusal is None:
                        kinds._unpack(bundle, tree)
                    else:
                        with self.assertRaises(SystemExit) as e:
                            kinds._unpack(bundle, tree)
                        self.assertIn(refusal, str(e.exception))
                landed = sorted(p.name for p in (tree / "data/cache").iterdir()
                                ) if (tree / "data/cache").exists() else []
                # The offending member is refused *before* it is written, so
                # the second file never reaches the disk.
                self.assertEqual(
                    landed, ["a.json", "b.json"] if refusal is None
                    else ["a.json"])

    def test_a_pull_past_an_expansion_limit_keeps_the_local_tree(self):
        # Through the real pull: the refusal has to leave the existing
        # artifact alone and take its staging directory with it.
        for name in ("1.json", "2.json"):
            self.write(f"data/cache/{name}", "{}")
        remote, man = Fake(), Manifest({})
        ctx = Ctx(self.cfg, remote, man)
        kinds.archive_push(ctx, self.art())
        self.write("data/cache/1.json", "irreplaceable local edit")
        before = self._tree()

        with unittest.mock.patch.object(kinds, "MAX_MEMBERS", 1):
            with self.assertRaises(SystemExit) as e:
                kinds.archive_pull(ctx, self.art(), clean=True)
        self.assertIn("holds more than 1 members", str(e.exception))
        self.assertEqual(self._tree(), before)
        self.assertEqual(
            list((config.state_dir(self.root) / "tmp").glob("stage-*")), [])

    def test_a_pull_of_a_bundle_that_climbs_out_is_refused_intact(self):
        # The same hostile bundle through the real pull: a reader is told the
        # bundle would not unpack and the local tree is exactly as it was.
        self.write("data/cache/1.json", "{}")
        remote, man = Fake(), Manifest({})
        ctx = Ctx(self.cfg, remote, man)
        kinds.archive_push(ctx, self.art())
        before = tree_hash(self.root / "data/cache")

        climbing = tarfile.TarInfo("../../../../escaped.txt")
        climbing.size = 7
        body = self._crafted_bundle(
            self.root / "hostile.tar.zst", [(climbing, b"CHANGED")]
        ).read_bytes()
        (self.root / "hostile.tar.zst").unlink()
        remote.objects["v1/data-cache.tar.zst"] = body
        man.artifacts["cache"]["archive_sha256"] = hashlib.sha256(
            body).hexdigest()
        man.artifacts["cache"]["archive_bytes"] = len(body)

        with self.assertRaises(SystemExit) as e:
            kinds.archive_pull(ctx, self.art(), force=True)
        self.assertIn("would not unpack", str(e.exception))
        self.assertIn("was not touched", str(e.exception))
        self.assertEqual(tree_hash(self.root / "data/cache"), before)
        self.assertFalse((self.root / "escaped.txt").exists())

    def test_clean_pull_keeps_a_symlinked_artifact_directory(self):
        outside = Path(tempfile.mkdtemp(prefix="litmo-outside-")).resolve()
        self.addCleanup(shutil.rmtree, outside, ignore_errors=True)
        (self.root / "data").mkdir()
        (self.root / "data/cache").symlink_to(outside)
        self.write("data/cache/1.json", "{}")

        remote, man = Fake(), Manifest({})
        ctx = Ctx(self.cfg, remote, man)
        kinds.archive_push(ctx, self.art())
        self.write("data/cache/stale.json", "{}")
        kinds.archive_pull(ctx, self.art(), clean=True)

        self.assertTrue((self.root / "data/cache").is_symlink())
        self.assertEqual({p.name for p in outside.iterdir()}, {"1.json"})

    def test_a_tree_rewritten_while_it_packs_is_not_published(self):
        # tree_hash and _pack read the tree separately, so a build still
        # running under a publish can put different bytes in each — and the
        # manifest would record a digest the uploaded bundle does not have.
        for i in range(3):
            self.write(f"data/cache/{i}.json", f'{{"n": {i}}}')
        remote, man = Fake(), Manifest({})
        ctx = Ctx(self.cfg, remote, man)

        real_pack = kinds._pack

        def racing_pack(root, src, dest):
            self.write("data/cache/3.json", "{}")     # the build is still going
            real_pack(root, src, dest)

        with unittest.mock.patch.object(kinds, "_pack", racing_pack):
            with self.assertRaises(SystemExit) as e:
                kinds.archive_push(ctx, self.art())
        self.assertIn("changed while it was being packed", str(e.exception))
        self.assertEqual(remote.objects, {})
        self.assertIsNone(man.get("cache"))

    def test_a_pack_race_undone_before_the_re_read_is_still_refused(self):
        # The sibling of the test above, and the one the re-read alone could
        # not catch: a file rewritten in place while `_pack` was reading it
        # and put back before the second `tree_hash` leaves both hashes
        # agreeing on bytes the bundle does not hold. The push reported
        # success, the manifest recorded the digest of the tree of `A`, the
        # bundle held `B`, and every reader's pull then failed for good with
        # "the bundle is not the tree the manifest describes".
        f = self.write("data/cache/x.json", "A" * 16)
        remote, man = Fake(), Manifest({})
        ctx = Ctx(self.cfg, remote, man)
        real_pack = kinds._pack

        def racing_pack(root, src, dest):
            f.write_text("B" * 16)          # the build rewrites, in place …
            real_pack(root, src, dest)
            f.write_text("A" * 16)          # … and puts it back

        before = tree_hash(self.root / "data/cache")
        with unittest.mock.patch.object(kinds, "_pack", racing_pack):
            with self.assertRaises(SystemExit) as e:
                kinds.archive_push(ctx, self.art())

        # The bytes are back to what they were, so only the timestamps could
        # have told anyone — and the message has to say that rather than
        # print the same digest twice as "was" and "now".
        self.assertEqual(tree_hash(self.root / "data/cache"), before)
        self.assertIn("changed while it was being packed", str(e.exception))
        self.assertIn("bytes are back to what they were", str(e.exception))
        self.assertNotIn("was " + before[0], str(e.exception))
        self.assertEqual(remote.objects, {})
        self.assertIsNone(man.get("cache"))

    def test_a_push_with_nothing_to_do_takes_no_extra_walk(self):
        # `stamps` is a whole walk of the artifact, so it belongs after the
        # up-to-date return, not beside the `tree_hash` above it: a no-op
        # push is the floor under `make publish` and must not pay for it.
        for i in range(3):
            self.write(f"data/cache/{i}.json", f'{{"n": {i}}}')
        remote, man = Fake(), Manifest({})
        ctx = Ctx(self.cfg, remote, man)
        kinds.archive_push(ctx, self.art())

        walks = []
        real_stamps = kinds.stamps
        with unittest.mock.patch.object(
                kinds, "stamps",
                lambda root: walks.append(root) or real_stamps(root)):
            self.assertFalse(kinds.archive_push(ctx, self.art()))
        self.assertEqual(walks, [])

    def test_a_symlink_out_of_the_artifact_is_refused_at_push(self):
        # tar stores the link as a link; tree_hash reads through it. So the
        # bundle would carry a reference to a path only this machine has,
        # while the manifest carried the target's bytes — push exit 0,
        # status "in sync", and every reader's pull failing on the `data`
        # extraction filter.
        outside = Path(tempfile.mkdtemp(prefix="litmo-outside-")).resolve()
        self.addCleanup(shutil.rmtree, outside, ignore_errors=True)
        (outside / "big.bin").write_text("BIG")
        self.write("data/cache/1.json", "{}")
        (self.root / "data/cache/big.bin").symlink_to(outside / "big.bin")

        remote, man = Fake(), Manifest({})
        ctx = Ctx(self.cfg, remote, man)
        with self.assertRaises(SystemExit) as e:
            kinds.archive_push(ctx, self.art())
        self.assertIn("nothing was uploaded", str(e.exception))
        self.assertIn("data/cache/big.bin", str(e.exception))
        self.assertEqual(remote.objects, {})
        self.assertIsNone(man.get("cache"))

    def test_a_symlinked_directory_out_of_the_artifact_is_refused_too(self):
        # Worse than a file link: rglob does not descend through it, so the
        # tree hash does not even see the contents it would publish.
        outside = Path(tempfile.mkdtemp(prefix="litmo-outside-")).resolve()
        self.addCleanup(shutil.rmtree, outside, ignore_errors=True)
        (outside / "x.json").write_text("{}")
        self.write("data/cache/1.json", "{}")
        (self.root / "data/cache/sub").symlink_to(outside)

        ctx = Ctx(self.cfg, Fake(), Manifest({}))
        with self.assertRaises(SystemExit) as e:
            kinds.archive_push(ctx, self.art())
        self.assertIn("data/cache/sub", str(e.exception))

    def test_a_fifo_in_the_artifact_is_refused_at_push(self):
        # tree_hash counts regular files only, so a fifo never moves the
        # digest — while tarfile packs it faithfully and every reader's
        # extraction filter then refuses the whole bundle. The push used to
        # exit 0 reporting "1 files" and status said in sync; worse, the next
        # push said "up to date" and never repacked, so the bucket stayed
        # unreadable until someone passed --force.
        self.write("data/cache/1.json", "{}")
        os.mkfifo(self.root / "data/cache/pipe")

        remote, man = Fake(), Manifest({})
        ctx = Ctx(self.cfg, remote, man)
        with self.assertRaises(SystemExit) as e:
            kinds.archive_push(ctx, self.art())
        self.assertIn("file(s) that are not files", str(e.exception))
        self.assertIn("data/cache/pipe", str(e.exception))
        self.assertEqual(remote.objects, {})
        self.assertIsNone(man.get("cache"))

    def test_the_refusal_survives_a_bucket_that_is_already_up_to_date(self):
        # The check has to come before the tree-hash early return, or the one
        # publisher who could fix a broken bucket is told nothing.
        self.write("data/cache/1.json", "{}")
        remote, man = Fake(), Manifest({})
        ctx = Ctx(self.cfg, remote, man)
        kinds.archive_push(ctx, self.art())
        os.mkfifo(self.root / "data/cache/pipe")

        with self.assertRaises(SystemExit) as e:
            kinds.archive_push(ctx, self.art())
        self.assertIn("data/cache/pipe", str(e.exception))

    def test_a_fifo_that_appears_while_packing_is_refused(self):
        self.write("data/cache/1.json", "{}")
        real_check = kinds._unpublishable

        def racing_check(src):
            out = real_check(src)
            os.mkfifo(self.root / "data/cache/pipe")
            return out

        remote, man = Fake(), Manifest({})
        ctx = Ctx(self.cfg, remote, man)
        with unittest.mock.patch.object(kinds, "_unpublishable", racing_check):
            with self.assertRaises(SystemExit) as e:
                kinds.archive_push(ctx, self.art())
        self.assertIn("data/cache/pipe became a fifo", str(e.exception))
        self.assertEqual(remote.objects, {})

    def test_a_socket_is_not_a_special_file_this_has_to_refuse(self):
        # tarfile declines to pack a socket at all, so nothing goes into the
        # bundle and nothing has to come back out. Refusing it too would
        # block a push that works today.
        self.write("data/cache/1.json", "{}")
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.addCleanup(sock.close)
        sock.bind(str(self.root / "data/cache/sock"))
        self.assertTrue((self.root / "data/cache/sock").is_socket())

        remote, man = Fake(), Manifest({})
        ctx = Ctx(self.cfg, remote, man)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            self.assertTrue(kinds.archive_push(ctx, self.art()))
        shutil.rmtree(self.root / "data/cache")
        kinds.archive_pull(ctx, self.art())
        self.assertEqual((self.root / "data/cache/1.json").read_text(), "{}")

    def test_a_symlink_that_appears_while_packing_is_refused(self):
        # The preflight check runs once, before the hash. A build still
        # running underneath the publish can replace a regular file with an
        # external link after it, and nothing downstream sees the difference:
        # tree_hash reads *through* the link both times, so the race re-read
        # agrees with itself while tar stores a reference to a path only this
        # machine has. Push exited 0, status said in sync, and every reader
        # got AbsoluteLinkError.
        outside = Path(tempfile.mkdtemp(prefix="litmo-outside-")).resolve()
        self.addCleanup(shutil.rmtree, outside, ignore_errors=True)
        (outside / "big.bin").write_text("someone else's bytes")
        self.write("data/cache/1.json", "{}")
        self.write("data/cache/big.bin", "someone else's bytes")

        real_check = kinds._unpublishable

        def racing_check(src):
            out = real_check(src)              # the check itself is honest …
            p = self.root / "data/cache/big.bin"   # … the build lands after it
            p.unlink()
            p.symlink_to(outside / "big.bin")
            return out

        remote, man = Fake(), Manifest({})
        ctx = Ctx(self.cfg, remote, man)
        with unittest.mock.patch.object(kinds, "_unpublishable", racing_check):
            with self.assertRaises(SystemExit) as e:
                kinds.archive_push(ctx, self.art())
        self.assertIn("data/cache/big.bin became a symlink", str(e.exception))
        self.assertIn("nothing was uploaded", str(e.exception))
        self.assertEqual(remote.objects, {})
        self.assertIsNone(man.get("cache"))

    def test_a_directory_symlink_that_appears_while_packing_is_refused_too(self):
        outside = Path(tempfile.mkdtemp(prefix="litmo-outside-")).resolve()
        self.addCleanup(shutil.rmtree, outside, ignore_errors=True)
        (outside / "x.json").write_text("{}")
        self.write("data/cache/1.json", "{}")
        self.write("data/cache/sub/x.json", "{}")

        real_check = kinds._unpublishable

        def racing_check(src):
            out = real_check(src)
            shutil.rmtree(self.root / "data/cache/sub")
            (self.root / "data/cache/sub").symlink_to(outside)
            return out

        remote, man = Fake(), Manifest({})
        ctx = Ctx(self.cfg, remote, man)
        with unittest.mock.patch.object(kinds, "_unpublishable", racing_check):
            with self.assertRaises(SystemExit) as e:
                kinds.archive_push(ctx, self.art())
        self.assertIn("data/cache/sub became a symlink", str(e.exception))
        self.assertEqual(remote.objects, {})

    def test_a_symlink_inside_the_artifact_still_round_trips(self):
        self.write("data/cache/real.json", "{}")
        (self.root / "data/cache/link.json").symlink_to("real.json")
        remote, man = Fake(), Manifest({})
        ctx = Ctx(self.cfg, remote, man)
        self.assertTrue(kinds.archive_push(ctx, self.art()))
        self.assertEqual(kinds.archive_status(ctx, self.art()).verdict,
                         "in sync")

        shutil.rmtree(self.root / "data/cache")
        kinds.archive_pull(ctx, self.art())
        self.assertEqual((self.root / "data/cache/link.json").read_text(), "{}")

    def test_a_merge_that_would_fail_partway_is_refused_first(self):
        # A merge moves file by file, so a path that changed between a file
        # and a directory raises after some of them have already landed. The
        # conflict is looked for before the first move instead.
        self.write("data/cache/a.json", "published")
        self.write("data/cache/foo", "published")
        remote, man = Fake(), Manifest({})
        ctx = Ctx(self.cfg, remote, man)
        kinds.archive_push(ctx, self.art())

        (self.root / "data/cache/foo").unlink()
        self.write("data/cache/foo/x.json", "irreplaceable")
        self.write("data/cache/a.json", "local")
        before = tree_hash(self.root / "data/cache")

        with self.assertRaises(SystemExit) as e:
            kinds.archive_pull(ctx, self.art())
        self.assertIn("data/cache was not touched", str(e.exception))
        self.assertIn("data/cache/foo", str(e.exception))
        self.assertEqual(tree_hash(self.root / "data/cache"), before)

    def test_a_merge_onto_a_file_that_became_a_directory_is_refused(self):
        self.write("data/cache/a.json", "published")
        self.write("data/cache/foo/x.json", "published")
        remote, man = Fake(), Manifest({})
        ctx = Ctx(self.cfg, remote, man)
        kinds.archive_push(ctx, self.art())

        shutil.rmtree(self.root / "data/cache/foo")
        self.write("data/cache/foo", "was a file")
        self.write("data/cache/a.json", "local")
        before = tree_hash(self.root / "data/cache")

        with self.assertRaises(SystemExit) as e:
            kinds.archive_pull(ctx, self.art())
        self.assertIn("data/cache was not touched", str(e.exception))
        self.assertEqual(tree_hash(self.root / "data/cache"), before)

    def test_a_merge_may_not_replace_the_artifact_root_outright(self):
        # The whole-tree swap renames the local copy into the attic, which
        # goes with the staging tree. Without --clean that is a merge
        # destroying what it did not download — silently, reporting success.
        self.write("data/cache/1.json", "{}")
        remote, man = Fake(), Manifest({})
        ctx = Ctx(self.cfg, remote, man)
        kinds.archive_push(ctx, self.art())

        shutil.rmtree(self.root / "data/cache")
        (self.root / "data/cache").write_text("irreplaceable local file")
        with self.assertRaises(SystemExit) as e:
            kinds.archive_pull(ctx, self.art())
        self.assertIn("the whole artifact would be replaced", str(e.exception))
        self.assertEqual((self.root / "data/cache").read_text(),
                         "irreplaceable local file")

        # --clean is what asks for the bucket's copy, and still gets it.
        kinds.archive_pull(ctx, self.art(), clean=True)
        self.assertEqual((self.root / "data/cache/1.json").read_text(), "{}")

    def test_a_merge_may_not_replace_an_artifact_tree_with_a_file(self):
        self.write("data/cache", "published as one file")
        remote, man = Fake(), Manifest({})
        ctx = Ctx(self.cfg, remote, man)
        kinds.archive_push(ctx, self.art())

        (self.root / "data/cache").unlink()
        self.write("data/cache/mine.json", "irreplaceable local tree")
        with self.assertRaises(SystemExit) as e:
            kinds.archive_pull(ctx, self.art())
        self.assertIn("the whole artifact would be replaced", str(e.exception))
        self.assertEqual((self.root / "data/cache/mine.json").read_text(),
                         "irreplaceable local tree")

    def _diverged_clean_pull(self):
        """Publish a tree, then let the local copy diverge. Returns (ctx, art
        callable's ctx) with a `--clean` pull left to do."""
        self.write("data/cache/published.json", "published")
        remote, man = Fake(), Manifest({})
        ctx = Ctx(self.cfg, remote, man)
        kinds.archive_push(ctx, self.art())
        self.write("data/cache/published.json", "irreplaceable local edit")
        self.write("data/cache/only-here.json", "also irreplaceable")
        return ctx

    def _tree(self):
        d = self.root / "data/cache"
        return {p.relative_to(d).as_posix(): p.read_text()
                for p in sorted(d.rglob("*")) if p.is_file()}

    def test_a_clean_pull_whose_install_fails_keeps_the_previous_tree(self):
        # The swap is two steps, and everything the second one can raise —
        # ENOSPC, EIO, a concurrent layout change, a cross-filesystem copy
        # that dies partway — used to arrive after the old tree had already
        # been renamed into the staging directory, which is then deleted.
        ctx = self._diverged_clean_pull()
        before = self._tree()

        real_move = kinds._move

        def failing_move(src, dest):
            if src.name == "cache":            # the staged tree -> destination
                raise OSError(errno.EIO, "I/O error")
            return real_move(src, dest)

        with unittest.mock.patch.object(kinds, "_move", failing_move):
            with self.assertRaises(OSError):
                kinds.archive_pull(ctx, self.art(), clean=True)

        self.assertEqual(self._tree(), before)
        self.assertEqual([p.name for p in (self.root / "data").iterdir()],
                         ["cache"])           # nothing parked left behind

    def test_a_clean_pull_sweeps_a_park_a_killed_run_left_behind(self):
        ctx = self._diverged_clean_pull()
        stale = self.root / "data/.cache.litmo-old"
        stale.mkdir()
        (stale / "from-a-dead-run.json").write_text("{}")

        kinds.archive_pull(ctx, self.art(), clean=True)
        self.assertEqual(self._tree(), {"published.json": "published"})
        self.assertEqual([p.name for p in (self.root / "data").iterdir()],
                         ["cache"])

    def test_a_failed_retry_after_a_killed_install_keeps_the_only_copy(self):
        # The park sweep above is only correct while a live destination
        # exists: that park is then a superseded generation. A run killed
        # between `os.replace(dest, parked)` and the move leaves the park
        # holding the *only* local copy, with nothing at the destination —
        # and sweeping it before finding that out, then failing the move,
        # left neither. Both irreplaceable files, gone, on a retry.
        ctx = self._diverged_clean_pull()
        before = self._tree()
        dest, park = self.root / "data/cache", self.root / "data/.cache.litmo-old"
        os.replace(dest, park)                 # the killed run's leftovers
        self.assertFalse(dest.exists())

        real_move = kinds._move

        def failing_move(src, d):
            if src.name == "cache":            # the staged tree -> destination
                raise OSError(errno.EIO, "I/O error")
            return real_move(src, d)

        with unittest.mock.patch.object(kinds, "_move", failing_move):
            with self.assertRaises(OSError):
                kinds.archive_pull(ctx, self.art(), clean=True)

        self.assertTrue(dest.is_dir(), "the only local copy was deleted")
        self.assertEqual(self._tree(), before)
        self.assertEqual([p.name for p in (self.root / "data").iterdir()],
                         ["cache"])           # nothing parked left behind

    def test_a_retry_after_a_killed_install_still_installs_and_sweeps(self):
        # The other half: adopting the park must not stop the retry from
        # succeeding, nor leave the park behind once it has.
        ctx = self._diverged_clean_pull()
        dest, park = self.root / "data/cache", self.root / "data/.cache.litmo-old"
        os.replace(dest, park)

        kinds.archive_pull(ctx, self.art(), clean=True)

        self.assertEqual(self._tree(), {"published.json": "published"})
        self.assertEqual([p.name for p in (self.root / "data").iterdir()],
                         ["cache"])

    @unittest.skipIf(kinds.fcntl is None, "no POSIX file locks here")
    def test_two_concurrent_installs_cannot_destroy_the_artifact(self):
        # Two pulls of one artifact shared `.cache.litmo-old`: the second read
        # the first's park as a dead run's leftover and deleted it, then
        # installed into the destination the first had just vacated. The
        # first's rename failed ENOTEMPTY, its rollback deleted the second's
        # tree and had no park left to restore, and `data/cache` ended up
        # absent — with the second pull having reported success.
        self.write("data/cache/original.json", "ORIGINAL")
        dest = self.root / "data/cache"
        park = self.root / "data/.cache.litmo-old"
        staged = {}
        for tag in ("P1", "P2"):
            s = self.root / f"stage-{tag}"
            s.mkdir()
            (s / f"{tag}.json").write_text(tag)
            staged[tag] = s

        real_move, parked, let_go = kinds._move, threading.Event(), threading.Event()

        def move(src, d):
            if src == staged["P1"]:     # P1 holds the lock and has parked
                parked.set()
                let_go.wait(10)
            return real_move(src, d)

        errors = {}

        def install(tag):
            try:
                with kinds._installing(self.cfg, self.art()):
                    kinds._install(staged[tag], dest, clean=True)
            except BaseException as e:                        # noqa: BLE001
                errors[tag] = f"{type(e).__name__}: {e}"

        with unittest.mock.patch.object(kinds, "_move", move):
            t1 = threading.Thread(target=install, args=("P1",))
            t2 = threading.Thread(target=install, args=("P2",))
            t1.start()
            self.assertTrue(parked.wait(10))
            t2.start()
            # P2 must be waiting on the lock rather than sweeping P1's park.
            t2.join(0.5)
            self.assertTrue(t2.is_alive())
            self.assertTrue(park.is_dir())
            self.assertEqual([p.name for p in park.iterdir()],
                             ["original.json"])
            let_go.set()
            t1.join(10)
            t2.join(10)

        self.assertEqual(errors, {})
        self.assertTrue(dest.is_dir())
        self.assertEqual(sorted(p.name for p in dest.iterdir()), ["P2.json"])
        self.assertFalse(park.exists())

    @unittest.skipIf(kinds.fcntl is None, "no POSIX file locks here")
    def test_an_install_that_cannot_take_the_lock_refuses_rather_than_hangs(self):
        # flock is per open file description, so a second acquisition from
        # this same process contends exactly as another process would.
        (self.root / "data/cache").mkdir(parents=True)
        with kinds._installing(self.cfg, self.art()), \
                unittest.mock.patch.object(kinds, "INSTALL_WAIT", 0.2):
            start = time.monotonic()
            with self.assertRaises(SystemExit) as e:
                with kinds._installing(self.cfg, self.art()):
                    self.fail("took a lock another install was holding")
            self.assertLess(time.monotonic() - start, 10)
        self.assertIn("another litmo has been installing", str(e.exception))
        self.assertIn("was not touched", str(e.exception))

    @unittest.skipIf(kinds.fcntl is None, "no POSIX file locks here")
    def test_a_pull_installs_under_the_lock_and_not_beside_it(self):
        # The two tests above prove `_installing` works; neither proves that
        # the thing which actually calls `_swap` ever asks for it. Replacing
        # `archive_pull`'s `_installing` with `contextlib.nullcontext()` left
        # the whole suite green, which is exactly the regression that matters:
        # `_swap`'s park has a fixed name, so an unserialised pull is the
        # concurrency bug `_installing` exists to close.
        ctx = self._diverged_clean_pull()
        before = self._tree()

        with kinds._installing(self.cfg, self.art()), \
                unittest.mock.patch.object(kinds, "INSTALL_WAIT", 0):
            with self.assertRaises(SystemExit) as e:
                kinds.archive_pull(ctx, self.art(), clean=True)
        self.assertIn("another litmo has been installing", str(e.exception))
        self.assertEqual(self._tree(), before)      # nothing was installed
        self.assertFalse((self.root / "data/.cache.litmo-old").exists())

        # Released: the same pull, unchanged, goes through.
        kinds.archive_pull(ctx, self.art(), clean=True)
        self.assertEqual(self._tree(), {"published.json": "published"})

    @unittest.skipUnless(OTHER_FS, "no second writable filesystem here")
    def test_a_clean_pull_onto_another_filesystem_swaps_and_rolls_back(self):
        # The artifact directory symlinked onto a scratch disk is the case
        # that made the park itself EXDEV. Nothing may cross a filesystem
        # boundary before the new tree is in place.
        elsewhere = Path(tempfile.mkdtemp(prefix="litmo-fs-", dir=OTHER_FS))
        parked = elsewhere.parent / f".{elsewhere.name}.litmo-old"
        self.addCleanup(shutil.rmtree, elsewhere, ignore_errors=True)
        self.addCleanup(shutil.rmtree, parked, ignore_errors=True)
        (self.root / "data").mkdir()
        (self.root / "data/cache").symlink_to(elsewhere)
        ctx = self._diverged_clean_pull()
        self.assertNotEqual(os.stat(self.root).st_dev, os.stat(elsewhere).st_dev)
        before = self._tree()

        def failing_copy(src, dst):            # the cross-filesystem fallback
            raise OSError(errno.ENOSPC, "No space left on device")

        with unittest.mock.patch.object(shutil, "move", failing_copy):
            with self.assertRaises(OSError):
                kinds.archive_pull(ctx, self.art(), clean=True)
        self.assertTrue((self.root / "data/cache").is_symlink())
        self.assertEqual(self._tree(), before)

        kinds.archive_pull(ctx, self.art(), clean=True)
        self.assertEqual(self._tree(), {"published.json": "published"})
        self.assertTrue((self.root / "data/cache").is_symlink())
        self.assertFalse(parked.exists())

    def _published_over_a_symlinked_subdirectory(self):
        """Publish `sub/x`, then make the reader's `sub` a link out of the
        artifact. Returns (ctx, outside)."""
        outside = Path(tempfile.mkdtemp(prefix="litmo-outside-")).resolve()
        self.addCleanup(shutil.rmtree, outside, ignore_errors=True)
        self.write("data/cache/sub/x", "FROM THE BUCKET")
        self.write("data/cache/keep.json", "published")
        remote, man = Fake(), Manifest({})
        ctx = Ctx(self.cfg, remote, man)
        kinds.archive_push(ctx, self.art())

        (outside / "x").write_text("irreplaceable")
        shutil.rmtree(self.root / "data/cache/sub")
        (self.root / "data/cache/sub").symlink_to(outside)
        self.write("data/cache/keep.json", "local")     # so it is a real merge
        return ctx, outside

    def test_a_merge_may_not_write_through_a_symlinked_parent(self):
        # `sub` is a link out of the artifact, and the bundle holds `sub/x`.
        # The merge joins its paths rather than resolving them, so without a
        # check here mkdir(exist_ok=True) accepts the link and the move writes
        # straight through it — outside the artifact, reporting success.
        ctx, outside = self._published_over_a_symlinked_subdirectory()

        with self.assertRaises(SystemExit) as e:
            kinds.archive_pull(ctx, self.art())
        self.assertIn("data/cache was not touched", str(e.exception))
        self.assertIn("data/cache/sub is a symlink here", str(e.exception))
        self.assertEqual((outside / "x").read_text(), "irreplaceable")
        self.assertEqual((self.root / "data/cache/keep.json").read_text(),
                         "local")

    def test_the_clean_pull_that_refusal_advises_stays_inside_too(self):
        # The refusal above says to re-run with --clean, so --clean must
        # actually be safe: it renames the whole local tree away, taking the
        # link with it, and never follows it.
        ctx, outside = self._published_over_a_symlinked_subdirectory()

        kinds.archive_pull(ctx, self.art(), clean=True)
        self.assertEqual((outside / "x").read_text(), "irreplaceable")
        self.assertFalse((self.root / "data/cache/sub").is_symlink())
        self.assertEqual((self.root / "data/cache/sub/x").read_text(),
                         "FROM THE BUCKET")

    @unittest.skipUnless(OTHER_FS, "no second writable filesystem here")
    def test_a_cross_filesystem_merge_replaces_a_symlink_rather_than_following(self):
        # `_blocked` lets a symlink AT the destination through, because
        # os.replace replaces the link itself. The cross-filesystem fallback
        # was not os.replace: shutil.move follows the link — into the
        # directory it names, or through it onto the file it names — so a
        # merge onto an artifact symlinked to another disk wrote outside it
        # and reported success.
        elsewhere = Path(tempfile.mkdtemp(prefix="litmo-fs-", dir=OTHER_FS))
        self.addCleanup(shutil.rmtree, elsewhere, ignore_errors=True)
        (self.root / "data").mkdir()
        (self.root / "data/cache").symlink_to(elsewhere)
        self.assertNotEqual(os.stat(self.root).st_dev, os.stat(elsewhere).st_dev)

        self.write("data/cache/keep.json", "published")
        self.write("data/cache/onto-a-file", "FROM THE BUCKET")
        self.write("data/cache/onto-a-dir", "FROM THE BUCKET")
        remote, man = Fake(), Manifest({})
        ctx = Ctx(self.cfg, remote, man)
        kinds.archive_push(ctx, self.art())

        outside = Path(tempfile.mkdtemp(prefix="litmo-outside-")).resolve()
        self.addCleanup(shutil.rmtree, outside, ignore_errors=True)
        (outside / "victim").write_text("irreplaceable")
        (outside / "sub").mkdir()
        for name, target in (("onto-a-file", "victim"), ("onto-a-dir", "sub")):
            (elsewhere / name).unlink()
            (elsewhere / name).symlink_to(outside / target)
        self.write("data/cache/keep.json", "local")     # so it is a merge

        kinds.archive_pull(ctx, self.art())
        self.assertEqual((outside / "victim").read_text(), "irreplaceable")
        self.assertEqual(list((outside / "sub").iterdir()), [])
        for name in ("onto-a-file", "onto-a-dir"):
            self.assertFalse((elsewhere / name).is_symlink())
            self.assertEqual((elsewhere / name).read_text(), "FROM THE BUCKET")
        self.assertEqual([p.name for p in elsewhere.iterdir()
                          if p.name.endswith(".litmo-part")], [])

    @unittest.skipUnless(OTHER_FS, "no second writable filesystem here")
    def test_a_cross_filesystem_merge_installs_every_published_link(self):
        # A merge now moves links as well as files, so the EXDEV fallback in
        # `_move` carries them too — and it has to arrive as a link with the
        # same target, not as a copy of whatever it names, and not fail
        # outright on one that names nothing.
        elsewhere = Path(tempfile.mkdtemp(prefix="litmo-fs-", dir=OTHER_FS))
        self.addCleanup(shutil.rmtree, elsewhere, ignore_errors=True)
        (self.root / "data").mkdir()
        (self.root / "data/cache").symlink_to(elsewhere)
        self.assertNotEqual(os.stat(self.root).st_dev, os.stat(elsewhere).st_dev)

        ctx = self._mixed_link_tree()
        for p in elsewhere.iterdir():                # a reader's fresh copy
            if p.is_dir() and not p.is_symlink():
                shutil.rmtree(p)
            else:
                p.unlink()
        (elsewhere / "mine.json").write_text("MINE")

        kinds.archive_pull(ctx, self.art())

        self.assertEqual((elsewhere / "mine.json").read_text(), "MINE")
        self.assertEqual(self._links(), PUBLISHED_LINKS)
        self.assertEqual([p.name for p in elsewhere.iterdir()
                          if p.name.endswith(".litmo-part")], [])

    def test_a_merge_still_replaces_a_symlink_it_publishes_over(self):
        # The guard is about links on the way *to* a file, not at it: a
        # published `link.json` where a link sits locally is replaced, and an
        # untouched symlinked subdirectory the bundle names nothing under is
        # nobody's business.
        self.write("data/cache/real.json", "published")
        self.write("data/cache/link.json", "published")
        remote, man = Fake(), Manifest({})
        ctx = Ctx(self.cfg, remote, man)
        kinds.archive_push(ctx, self.art())

        outside = Path(tempfile.mkdtemp(prefix="litmo-outside-")).resolve()
        self.addCleanup(shutil.rmtree, outside, ignore_errors=True)
        (outside / "untouched").write_text("mine")
        (self.root / "data/cache/link.json").unlink()
        (self.root / "data/cache/link.json").symlink_to(outside / "untouched")
        (self.root / "data/cache/spare").symlink_to(outside)
        self.write("data/cache/real.json", "local")

        kinds.archive_pull(ctx, self.art())
        self.assertFalse((self.root / "data/cache/link.json").is_symlink())
        self.assertEqual((self.root / "data/cache/link.json").read_text(),
                         "published")
        self.assertEqual((outside / "untouched").read_text(), "mine")
        self.assertTrue((self.root / "data/cache/spare").is_symlink())

    def test_clean_pull_replaces_a_conflicting_shape_outright(self):
        # --clean swaps the whole tree rather than merging into it, so it has
        # no per-file conflict to hit and must keep working.
        self.write("data/cache/a.json", "published")
        self.write("data/cache/foo", "published")
        remote, man = Fake(), Manifest({})
        ctx = Ctx(self.cfg, remote, man)
        kinds.archive_push(ctx, self.art())
        after = tree_hash(self.root / "data/cache")

        (self.root / "data/cache/foo").unlink()
        self.write("data/cache/foo/x.json", "local")
        kinds.archive_pull(ctx, self.art(), clean=True)
        self.assertEqual(tree_hash(self.root / "data/cache"), after)

    def test_a_manifest_of_another_kind_is_refused_not_a_traceback(self):
        # sync.toml says archive; the bucket's entry is a mirror. Reaching
        # into it raised KeyError('tree_hash') from pull and a TypeError
        # formatting a list as a number from status.
        self.write("data/cache/1.json", "{}")
        man = Manifest({"cache": {
            "kind": "mirror", "path": "data/cache",
            "files": [{"path": "data/cache/1.json", "size": 2,
                       "sha256": H1}]}})
        ctx = Ctx(self.cfg, Fake(), man)
        for verb in (kinds.archive_status, kinds.archive_pull):
            with self.assertRaises(SystemExit) as e:
                verb(ctx, self.art())
            self.assertIn("declares this an archive", str(e.exception))

        # …and push is the way back: it replaces the entry rather than
        # refusing, so the bucket never becomes unwritable.
        self.assertTrue(kinds.archive_push(ctx, self.art()))
        self.assertEqual(man.get("cache")["kind"], "archive")

    def test_status_absent_both_sides(self):
        ctx = Ctx(self.cfg, Fake(), Manifest({}))
        self.assertEqual(kinds.archive_status(ctx, self.art()).verdict, "absent")

    # --- the size the download is held to ----------------------------------
    # `archive_bytes` is the only bound on the bundle before its sha-256 can
    # be checked, and the manifest is the untrusted side. A bucket that says
    # nothing, or says zero, used to hand `download` no limit at all.

    def _recording(self):
        """A bucket that remembers the cap each download was given."""
        remote = Fake()
        caps = []
        real = remote.download

        def download(key, dest, max_bytes=None):
            caps.append(max_bytes)
            return real(key, dest, max_bytes)

        remote.download = download
        return remote, caps

    def _published(self):
        self.write("data/cache/1.json", '{"n": 1}')
        remote, caps = self._recording()
        man = Manifest({})
        ctx = Ctx(self.cfg, remote, man)
        kinds.archive_push(ctx, self.art())
        return ctx, man, caps

    def test_the_download_is_held_to_the_size_the_manifest_promises(self):
        ctx, man, caps = self._published()
        promised = man.get("cache")["archive_bytes"]
        self.assertGreater(promised, 0)
        kinds.archive_pull(ctx, self.art(), force=True)
        self.assertEqual(caps, [promised])          # not None

    def test_an_archive_size_past_the_ceiling_is_refused_before_the_transfer(self):
        # The promise is the download's only bound, and the manifest is the
        # document being distrusted — Manifest takes any non-negative integer.
        # Measured against a server that never stops: 11.9 GB into staging in
        # three seconds with archive_bytes = 10**30. So the promise may
        # tighten the ceiling; it may not remove it.
        self.write("data/cache/1.json", "{}")
        remote, man = Fake(), Manifest({})
        ctx = Ctx(self.cfg, remote, man)
        kinds.archive_push(ctx, self.art())
        entry = dict(man.get("cache"))
        entry["archive_bytes"] = 10 ** 30
        man.set("cache", entry)

        asked = []
        remote.download = lambda k, d, m=None: asked.append(k)
        with self.assertRaises(SystemExit) as e:
            kinds.archive_pull(ctx, self.art(), force=True)
        self.assertIn("past the", str(e.exception))
        self.assertIn("data/cache was not touched", str(e.exception))
        self.assertEqual(asked, [])            # nothing was ever requested

    def test_an_archive_entry_promising_zero_bytes_is_refused(self):
        ctx, man, caps = self._published()
        before = tree_hash(self.root / "data/cache")
        man.artifacts["cache"]["archive_bytes"] = 0

        with self.assertRaises(SystemExit) as e:
            kinds.archive_pull(ctx, self.art(), force=True)
        self.assertIn("does not say how big", str(e.exception))
        self.assertIn("litmo push cache", str(e.exception))
        # Refused *before* the request, so no unbounded body is ever drained.
        self.assertEqual(caps, [])
        self.assertEqual(tree_hash(self.root / "data/cache"), before)

    def test_an_archive_entry_with_no_size_at_all_is_refused_by_name(self):
        # Absent rather than zero: this used to be a bare KeyError traceback.
        ctx, man, caps = self._published()
        del man.artifacts["cache"]["archive_bytes"]

        with self.assertRaises(SystemExit) as e:
            kinds.archive_pull(ctx, self.art(), force=True)
        self.assertIn("does not say how big", str(e.exception))
        self.assertEqual(caps, [])

    def test_a_bundle_longer_than_promised_never_reaches_the_disk(self):
        """End to end over the real `Remote`, which is what enforces the cap.

        `Fake.download` copies whatever it holds; only `remote._drain` counts
        bytes. So this one runs the public read path against a `file://`
        bucket holding a body far larger than the manifest admits to.
        """
        self.write("data/cache/1.json", '{"n": 1}')
        fake, man = Fake(), Manifest({})
        kinds.archive_push(Ctx(self.cfg, fake, man), self.art())
        before = tree_hash(self.root / "data/cache")

        bucket = Path(tempfile.mkdtemp(prefix="litmo-oversize-"))
        self.addCleanup(shutil.rmtree, bucket, ignore_errors=True)
        key = man.get("cache")["key"]
        (bucket / key).parent.mkdir(parents=True, exist_ok=True)
        (bucket / key).write_bytes(b"x" * (4 << 20))

        cfg = dataclasses.replace(self.cfg, base=bucket.as_uri())
        ctx = Ctx(cfg, Remote(cfg), man)
        with self.assertRaises(SystemExit) as e:
            kinds.archive_pull(ctx, self.art(), force=True)
        self.assertIn("longer than the", str(e.exception))
        self.assertEqual(tree_hash(self.root / "data/cache"), before)


# --- the fetch kind ---------------------------------------------------------

FETCH_TOML = """\
[artifact.positions]
kind = "fetch"
path = "data/positions.csv"
url  = "https://example.invalid/positions.csv"
"""


class TestFetch(Base):
    def setUp(self):
        super().setUp()
        self.cfg = self.reload(FETCH_TOML)
        self.art = self.cfg.artifacts[0]
        self.ctx = Ctx(self.cfg, Fake(), Manifest({}))
        self.served = b"one,two\n"
        self.headers = {"etag": "v1", "last_modified": "Mon, 01 Jan 2026"}
        self.fetches = 0

        def fake_head(url):
            return dict(self.headers, size=str(len(self.served)))

        def fake_fetch(url, dest):
            self.fetches += 1
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(self.served)
            return dict(self.headers)

        for name, fn in (("head", fake_head), ("fetch_url", fake_fetch)):
            original = getattr(kinds, name)
            setattr(kinds, name, fn)
            self.addCleanup(setattr, kinds, name, original)

    def test_an_oversized_fetch_is_reported_not_a_traceback(self):
        def refuses(url, dest):
            raise Oversized("larger than the 10 bytes a fetch is held to")

        kinds.fetch_url = refuses
        self.write("data/positions.csv", "mine\n")
        with self.assertRaises(SystemExit) as e:
            kinds.fetch_pull(self.ctx, self.art)
        self.assertIn("was not touched", str(e.exception))
        self.assertIn("a fetch is held to", str(e.exception))
        self.assertEqual((self.root / "data/positions.csv").read_text(), "mine\n")

    def test_first_pull_fetches_and_records(self):
        kinds.fetch_pull(self.ctx, self.art)
        self.assertEqual(self.fetches, 1)
        seen = self.ctx.state["fetch"]["positions"]
        self.assertEqual(seen["size"], len(self.served))
        self.assertEqual(kinds.fetch_status(self.ctx, self.art).verdict, "in sync")

    def test_an_unchanged_etag_is_not_refetched(self):
        kinds.fetch_pull(self.ctx, self.art)
        kinds.fetch_pull(self.ctx, self.art)
        self.assertEqual(self.fetches, 1)

    def test_a_locally_modified_file_is_not_in_sync(self):
        kinds.fetch_pull(self.ctx, self.art)
        (self.root / "data/positions.csv").write_bytes(b"edited by hand\n")
        r = kinds.fetch_status(self.ctx, self.art)
        self.assertEqual(r.verdict, "DIFFERS")
        self.assertIn("modified here", r.local)

    def test_a_locally_modified_file_is_refetched(self):
        kinds.fetch_pull(self.ctx, self.art)
        (self.root / "data/positions.csv").write_bytes(b"edited by hand\n")
        kinds.fetch_pull(self.ctx, self.art)
        self.assertEqual(self.fetches, 2)
        self.assertEqual((self.root / "data/positions.csv").read_bytes(),
                         self.served)

    def test_a_same_size_edit_is_still_caught(self):
        kinds.fetch_pull(self.ctx, self.art)
        (self.root / "data/positions.csv").write_bytes(b"XXX,two\n")
        self.assertEqual(kinds.fetch_status(self.ctx, self.art).verdict,
                         "DIFFERS")

    def test_a_state_file_from_before_digests_is_trusted(self):
        kinds.fetch_pull(self.ctx, self.art)
        self.ctx.state["fetch"]["positions"] = {"etag": "v1"}
        self.assertEqual(kinds.fetch_status(self.ctx, self.art).verdict,
                         "in sync")

    def test_a_new_etag_refetches_even_when_last_modified_holds(self):
        # The pair that matters: the bytes changed and the ETag says so, but
        # `Last-Modified` is one-second resolution and did not move. Treating
        # the two validators as alternatives kept the stale copy forever.
        kinds.fetch_pull(self.ctx, self.art)
        self.headers = {"etag": "v2", "last_modified": "Mon, 01 Jan 2026"}
        self.served = b"three,four\n"
        self.assertEqual(kinds.fetch_status(self.ctx, self.art).verdict,
                         "DIFFERS")
        kinds.fetch_pull(self.ctx, self.art)
        self.assertEqual(self.fetches, 2)
        self.assertEqual((self.root / "data/positions.csv").read_bytes(),
                         self.served)

    def test_a_new_last_modified_refetches_even_when_the_etag_holds(self):
        # The mirror image, for a server whose ETag is not derived from the
        # bytes (a weak or per-node one).
        kinds.fetch_pull(self.ctx, self.art)
        self.headers = {"etag": "v1", "last_modified": "Tue, 02 Jan 2026"}
        self.served = b"three,four\n"
        kinds.fetch_pull(self.ctx, self.art)
        self.assertEqual(self.fetches, 2)

    def test_a_server_offering_no_shared_validator_refetches(self):
        # Nothing to compare is not evidence of freshness.
        kinds.fetch_pull(self.ctx, self.art)
        self.headers = {}
        kinds.fetch_pull(self.ctx, self.art)
        self.assertEqual(self.fetches, 2)

    def test_a_new_etag_refetches(self):
        kinds.fetch_pull(self.ctx, self.art)
        self.headers = {"etag": "v2", "last_modified": "Tue, 02 Jan 2026"}
        self.served = b"three,four\n"
        kinds.fetch_pull(self.ctx, self.art)
        self.assertEqual(self.fetches, 2)


# --- local state ------------------------------------------------------------

class TestState(Base):
    def test_round_trip(self):
        kinds.save_state(self.cfg, {"fetch": {"x": {"etag": "1"}}})
        self.assertEqual(kinds.load_state(self.cfg)["fetch"]["x"]["etag"], "1")

    def test_write_is_atomic_and_leaves_no_scratch(self):
        kinds.save_state(self.cfg, {"a": 1})
        siblings = [p.name for p in self.cfg.state_file.parent.iterdir()]
        self.assertEqual(siblings, ["state.json"])

    def test_a_stale_staging_tree_is_swept_away(self):
        import os as _os
        stage = self.cfg.root / ".litmo/tmp/stage-abandoned"
        stage.mkdir(parents=True)
        (stage / "leftover").write_text("x" * 100)
        old = 1_600_000_000                       # long enough ago
        _os.utime(stage, (old, old))
        with kinds._staging(self.cfg):
            pass
        self.assertFalse(stage.exists())

    def test_a_fresh_staging_tree_is_left_alone(self):
        stage = self.cfg.root / ".litmo/tmp/stage-in-flight"
        stage.mkdir(parents=True)
        with kinds._staging(self.cfg):
            pass
        self.assertTrue(stage.exists())

    def test_a_pre_rename_state_directory_is_moved_not_abandoned(self):
        # Every checkout that ran the tool under its old name has `.litkit`.
        # Abandoning it re-fetches every input in every repository and makes
        # `status` report a tree nobody pulled.
        legacy = self.root / ".litkit"
        legacy.mkdir()
        (legacy / "state.json").write_text(
            json.dumps({"fetch": {"positions": {"etag": "keep-me"}}}))
        self.assertEqual(
            kinds.load_state(self.cfg)["fetch"]["positions"]["etag"], "keep-me")
        self.assertFalse(legacy.exists())
        self.assertTrue((self.root / ".litmo/state.json").exists())

    def test_a_pre_rename_directory_never_clobbers_the_current_one(self):
        # Both present means the tool has already run since the rename; the
        # new one is the live state and the stale one is not allowed over it.
        kinds.save_state(self.cfg, {"fetch": {"positions": {"etag": "live"}}})
        legacy = self.root / ".litkit"
        legacy.mkdir()
        (legacy / "state.json").write_text(
            json.dumps({"fetch": {"positions": {"etag": "stale"}}}))
        self.assertEqual(
            kinds.load_state(self.cfg)["fetch"]["positions"]["etag"], "live")
        self.assertTrue(legacy.exists())

    def test_a_leftover_pre_rename_directory_is_never_published(self):
        # A checkout that has not run since the rename still has `.litkit`,
        # and an artifact rooted above it would sweep the old state into the
        # bucket once the name stopped being skipped.
        art = config.Artifact(name="everything", kind="mirror", path=Path("."))
        self.assertFalse(kinds._covers(art, ".litkit/state.json"))
        self.assertFalse(kinds._covers(art, ".litmo/state.json"))
        self.assertTrue(kinds._covers(art, "out/a.csv"))

    def test_a_corrupt_state_file_is_not_fatal(self):
        self.cfg.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.cfg.state_file.write_text("{ truncated")
        self.assertEqual(kinds.load_state(self.cfg), {})

    def test_a_state_file_that_is_not_an_object_is_not_fatal(self):
        self.cfg.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.cfg.state_file.write_text("[1, 2, 3]")
        self.assertEqual(kinds.load_state(self.cfg), {})


# --- transport --------------------------------------------------------------

class Trickling:
    """A body arriving a byte at a time, with a clock that only moves when
    someone reads — a server sending just often enough that the socket
    timeout never fires.

    `read` raises on purpose. `_chunks` has to stream with `read1`: `read`
    does not return until it has the whole amount asked for, so a body like
    this spends the entire transfer inside one call and no check wrapped
    around that call ever runs.
    """

    def __init__(self, n: int, gap: float, clock: list):
        self.left, self.gap, self.clock = n, gap, clock
        self.headers: dict = {}

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def read(self, n: int = -1) -> bytes:
        raise AssertionError("_chunks must stream with read1, not read")

    def read1(self, n: int = -1) -> bytes:
        if not self.left:
            return b""
        self.left -= 1
        self.clock[0] += self.gap
        return b"A"


class Response:
    """Enough of an HTTP response for `_drain` and `get_bytes`."""

    def __init__(self, body: bytes, headers: dict | None = None):
        self._body, self.headers = body, headers or {}

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def read(self, n: int = -1) -> bytes:
        if n is None or n < 0:
            out, self._body = self._body, b""
            return out
        out, self._body = self._body[:n], self._body[n:]
        return out

    def read1(self, n: int = -1) -> bytes:
        """One underlying read, as `http.client.HTTPResponse` has. `_chunks`
        streams with this rather than `read`, which does not return until it
        has the whole amount asked for."""
        return self.read(n)


class TestRemote(Base):
    def served(self):
        """A `file://` base, so the public read path runs with no server."""
        d = Path(tempfile.mkdtemp(prefix="litmo-served-"))
        self.addCleanup(shutil.rmtree, d, ignore_errors=True)
        return d, Remote(dataclasses.replace(self.cfg, base=d.as_uri()))

    def test_a_body_that_trickles_is_refused_rather_than_streamed_forever(self):
        # `TIMEOUT` bounds one socket read, not a transfer. A server sending
        # a byte just before each one expires held a request open for as long
        # as it cared to and nothing ever raised, so `_retrying` never got a
        # say: measured against a real local server, `fetch_url` returned
        # successfully after 7.96 s with the timeout set to 0.1 s.
        clock = [0.0]
        body = Trickling(10_000, transport.TIMEOUT, clock)
        with unittest.mock.patch("time.monotonic", lambda: clock[0]):
            with self.assertRaises(TimeoutError) as e:
                list(transport._chunks(body))
        self.assertIn("stalled", str(e.exception))
        self.assertGreater(body.left, 0)          # it stopped, it did not run

    def test_a_slow_but_real_transfer_is_not_refused(self):
        # Why the bound is a rate and not the deadline the report asked for:
        # what comes down this path runs from a 157 KB manifest to a 2.18 GB
        # mirror, so a deadline short enough to catch a trickle would refuse
        # a large download over a slow link, and one generous enough for that
        # would catch nothing.
        clock = [0.0]

        class Slow(Trickling):
            def read1(self, n=-1):
                if not self.left:
                    return b""
                take = min(self.left, transport.MIN_RATE * 8)
                self.left -= take
                self.clock[0] += 1.0                    # 8 KiB a second
                return b"A" * take

        want = transport.MIN_RATE * 8 * 600             # ten minutes of it
        body = Slow(want, 0, clock)
        with unittest.mock.patch("time.monotonic", lambda: clock[0]):
            got = sum(len(c) for c in transport._chunks(body))
        self.assertEqual(got, want)
        self.assertGreater(clock[0], transport.TIMEOUT)  # past the grace

    def test_a_manifest_that_trickles_is_refused_and_costs_only_attempts(self):
        # The manifest is the first thing every reader asks for, so a server
        # that trickles it hangs the pull before anything else has happened.
        # `get_bytes` reads it outside `_drain`, so it needs its own pass
        # through `_chunks`.
        _, remote = self.served()
        clock, served = [0.0], []

        def urlopen(req, timeout=None):
            served.append(Trickling(10_000, transport.TIMEOUT, clock))
            return served[-1]

        with unittest.mock.patch.object(transport.urllib.request, "urlopen",
                                        urlopen), \
                unittest.mock.patch.object(transport, "BACKOFF", 0), \
                unittest.mock.patch("time.monotonic", lambda: clock[0]):
            with self.assertRaises(TimeoutError):
                remote.get_bytes("manifest.json")
        # A stall is transient, so it costs the attempt rather than the
        # process — which is the outcome the socket timeout never produced.
        self.assertEqual(len(served), transport.ATTEMPTS)

    @staticmethod
    def _rate_limited(retry_after=None, code=429):
        h = email.message.Message()
        if retry_after is not None:
            h["Retry-After"] = retry_after
        return urllib.error.HTTPError("http://x.invalid/o", code,
                                      "Too Many Requests", h, None)

    def _sleeps_for(self, err):
        """The pauses `_retrying` chooses when every attempt raises `err`."""
        slept = []
        with unittest.mock.patch.object(transport.time, "sleep", slept.append):
            with self.assertRaises(urllib.error.HTTPError):
                transport._retrying(lambda: (_ for _ in ()).throw(err), "o")
        return slept

    def test_a_rate_limit_waits_the_window_the_server_named(self):
        # `429` is classified transient, but the backoff was always its own:
        # a `Retry-After: 60` got three attempts and sleeps of 0.67 s and
        # 1.29 s — two seconds, every one of them inside the server's own
        # exclusion window — and then failed. With eight parallel downloads
        # that is every worker giving up during the window it was told about.
        self.assertEqual(self._sleeps_for(self._rate_limited("60")), [60, 60])
        self.assertEqual(self._sleeps_for(self._rate_limited("30", code=503)),
                         [30, 30])

    # A fixed instant to read the clock at, and the HTTP-date exactly 45
    # seconds after it, written out rather than computed.
    FROZEN_NOW = datetime.datetime(2026, 3, 1, 12, 0, 0, tzinfo=datetime.UTC)
    FROZEN_PLUS_45 = "Sun, 01 Mar 2026 12:00:45 GMT"

    def _clock_at(self, now):
        """Freeze the clock `remote._retry_after` reads.

        It answers with the server's date minus its own `now`, so a test that
        builds that date from the real clock is quietly asserting that
        nothing happens between the two reads — a scheduling pause, an ntp
        step, a loaded machine. Reproduced by advancing only the production
        side by three seconds: the answer came back 41.149121 against a
        two-second tolerance on 45, and the tolerance was there in the first
        place because formatting an HTTP-date drops the sub-second part.
        Frozen, the assertion is exact and the test cannot be made to fail by
        the machine it runs on.
        """
        shim = unittest.mock.Mock(UTC=datetime.UTC)
        shim.datetime.now.return_value = now
        return unittest.mock.patch.object(transport, "datetime", shim)

    def test_an_http_date_retry_after_is_read_as_well_as_delta_seconds(self):
        # RFC 9110 allows both forms and servers send both.
        with self._clock_at(self.FROZEN_NOW):
            asked = transport._retry_after(
                self._rate_limited(self.FROZEN_PLUS_45))
        self.assertEqual(asked, 45)

    def test_an_http_date_is_obeyed_by_the_retry_loop_as_well_as_parsed(self):
        # And it reaches the sleeps: `_retry_after` is only useful because
        # `_retrying` prefers it to its own backoff.
        with self._clock_at(self.FROZEN_NOW):
            self.assertEqual(
                self._sleeps_for(self._rate_limited(self.FROZEN_PLUS_45)),
                [45, 45])

    def test_a_retry_after_past_the_cap_is_capped_not_obeyed(self):
        # Honouring it unbounded is the opposite mistake: an hour would be
        # indistinguishable from a hang.
        self.assertEqual(self._sleeps_for(self._rate_limited("3600")),
                         [transport.RETRY_AFTER_MAX] * 2)

    def test_a_useless_retry_after_leaves_the_backoff_alone(self):
        # Zero, a date already past, and something neither form parses are
        # no instruction at all — and taking the *greater* of the two is what
        # stops a `Retry-After: 0` turning the retry into a hot loop.
        past = email.utils.format_datetime(
            datetime.datetime.now(datetime.UTC) - datetime.timedelta(hours=1))
        for value in ("0", "-5", past, "soon please", "", None):
            with self.subTest(value=value):
                slept = self._sleeps_for(self._rate_limited(value))
                self.assertEqual(len(slept), transport.ATTEMPTS - 1)
                for pause in slept:
                    self.assertGreater(pause, 0)
                    self.assertLess(pause, 5)

    def test_object_keys_are_percent_encoded(self):
        self.assertEqual(_url("https://x.invalid/m", "out/a b#c.csv"),
                         "https://x.invalid/m/out/a%20b%23c.csv")
        self.assertEqual(_url("https://x.invalid/m", "v1/x.tar.zst"),
                         "https://x.invalid/m/v1/x.tar.zst")

    def test_download(self):
        d, remote = self.served()
        (d / "a.csv").write_bytes(b"hello")
        remote.download("a.csv", self.root / "out/a.csv")
        self.assertEqual((self.root / "out/a.csv").read_bytes(), b"hello")

    def test_a_failed_download_leaves_no_part_file(self):
        _, remote = self.served()
        with self.assertRaises(urllib.error.URLError):
            remote.download("nope.csv", self.root / "out/a.csv")
        self.assertEqual(list((self.root / "out").iterdir()), [])

    def test_a_failed_download_cancels_the_queue(self):
        # The first failure decides the pull. Everything still queued behind
        # it would otherwise run anyway — at a socket timeout each, once the
        # network is what failed.
        _, remote = self.served()
        started = []

        def counting(key, dest, max_bytes=None):
            started.append(key)
            if key == "boom.csv":
                raise urllib.error.URLError("gone")
            time.sleep(0.02)

        remote.download = counting
        jobs = [("boom.csv", self.root / "out/boom.csv")]
        jobs += [(f"{i}.csv", self.root / f"out/{i}.csv") for i in range(40)]
        with self.assertRaises(urllib.error.URLError):
            remote.download_many(jobs, workers=1)
        self.assertLess(len(started), 10, started)

    def test_a_dropped_connection_is_retried(self):
        # The write path has ten botocore attempts; the read path most people
        # exercise used to have none, and one blip discarded a whole pull.
        _, remote = self.served()
        calls = []

        def flaky(req, timeout=None):
            calls.append(req.full_url)
            if len(calls) == 1:
                raise urllib.error.URLError(ConnectionResetError("reset"))
            return Response(b"hello")

        with unittest.mock.patch.object(transport.urllib.request, "urlopen",
                                        flaky), \
                unittest.mock.patch.object(transport, "BACKOFF", 0):
            remote.download("a.csv", self.root / "out/a.csv")
        self.assertEqual(len(calls), 2)
        self.assertEqual((self.root / "out/a.csv").read_bytes(), b"hello")

    def test_a_retry_does_not_append_to_the_previous_attempt(self):
        _, remote = self.served()
        calls = []

        def truncating(req, timeout=None):
            calls.append(req.full_url)
            if len(calls) == 1:
                return Response(b"half")     # closed before the body finished
            return Response(b"the whole thing")

        real_drain = transport._drain

        def drain(reader, fh, max_bytes):
            n = real_drain(reader, fh, max_bytes)
            if n < len(b"the whole thing"):
                raise http.client.IncompleteRead(b"")
            return n

        with unittest.mock.patch.object(transport.urllib.request, "urlopen",
                                        truncating), \
                unittest.mock.patch.object(transport, "_drain", drain), \
                unittest.mock.patch.object(transport, "BACKOFF", 0):
            remote.download("a.csv", self.root / "out/a.csv")
        self.assertEqual((self.root / "out/a.csv").read_bytes(),
                         b"the whole thing")

    def test_a_missing_object_is_not_retried(self):
        _, remote = self.served()
        calls = []

        def gone(req, timeout=None):
            calls.append(req.full_url)
            raise urllib.error.HTTPError(req.full_url, 404, "Not Found", {},
                                         None)

        with unittest.mock.patch.object(transport.urllib.request, "urlopen",
                                        gone):
            with self.assertRaises(Missing):
                remote.get_bytes("manifest.json")
        self.assertEqual(len(calls), 1)

    def test_an_oversized_body_is_not_retried(self):
        _, remote = self.served()
        calls = []

        def big(req, timeout=None):
            calls.append(req.full_url)
            return Response(b"x" * 100)

        with unittest.mock.patch.object(transport.urllib.request, "urlopen",
                                        big):
            with self.assertRaises(Oversized):
                remote.download("a.csv", self.root / "out/a.csv", 10)
        self.assertEqual(len(calls), 1)

    def test_a_body_longer_than_promised_is_refused(self):
        d, remote = self.served()
        (d / "a.csv").write_bytes(b"x" * 100)
        with self.assertRaises(Oversized):
            remote.download("a.csv", self.root / "out/a.csv", 10)
        self.assertEqual(list((self.root / "out").iterdir()), [])

    def test_a_fetch_is_bounded_though_nothing_promises_its_size(self):
        """`fetch_url` was the one read path with no bound at all.

        An archive is held to the size its manifest states; a `fetch` has no
        manifest, so a ceiling is the only bound there is. The url being
        trusted config does not supply one: `urlopen` follows redirects, so
        the body need not come from the configured host at all.
        """
        d = Path(tempfile.mkdtemp(prefix="litmo-fetch-"))
        self.addCleanup(shutil.rmtree, d, ignore_errors=True)
        (d / "big.csv").write_bytes(b"x" * 5000)
        dest = self.root / "data/positions.csv"

        with unittest.mock.patch.object(transport, "MAX_FETCH", 1000):
            with self.assertRaises(Oversized) as e:
                transport.fetch_url((d / "big.csv").as_uri(), dest)
        self.assertIn("a fetch is held to", str(e.exception))
        self.assertFalse(dest.exists())
        self.assertEqual(list(dest.parent.iterdir()), [])    # no .part either

    def test_a_fetch_inside_the_ceiling_still_arrives(self):
        d = Path(tempfile.mkdtemp(prefix="litmo-fetch-"))
        self.addCleanup(shutil.rmtree, d, ignore_errors=True)
        (d / "ok.csv").write_bytes(b"a,b\n1,2\n")
        dest = self.root / "data/positions.csv"
        transport.fetch_url((d / "ok.csv").as_uri(), dest)
        self.assertEqual(dest.read_bytes(), b"a,b\n1,2\n")

    def test_an_oversized_fetch_is_not_retried(self):
        calls = []

        def big(req, timeout=None):
            calls.append(req.full_url)
            return Response(b"x" * 5000)

        with unittest.mock.patch.object(transport.urllib.request, "urlopen",
                                        big), \
                unittest.mock.patch.object(transport, "MAX_FETCH", 10), \
                unittest.mock.patch.object(transport, "BACKOFF", 0):
            with self.assertRaises(Oversized):
                transport.fetch_url("https://x.invalid/a.csv",
                                    self.root / "data/positions.csv")
        self.assertEqual(len(calls), 1)


class StubS3:
    """A conditional write that is always refused, and the read-back after."""

    def __init__(self, stored: bytes | None, readable: bool = True):
        self.stored, self.readable, self.puts = stored, readable, 0

    def put_object(self, **kw):
        import botocore.exceptions
        self.puts += 1
        raise botocore.exceptions.ClientError(
            {"Error": {"Code": "PreconditionFailed"},
             "ResponseMetadata": {"HTTPStatusCode": 412}}, "PutObject")

    def get_object(self, Bucket, Key):
        if not self.readable:
            raise OSError("the connection is still down")
        return {"Body": Response(self.stored), "ETag": '"e"'}


class StubUnsupportedConditional:
    """Reject preconditions, but would accept an unsafe unconditional put."""

    def __init__(self):
        self.calls = []

    def put_object(self, **kw):
        import botocore.exceptions
        self.calls.append(kw)
        if "IfMatch" in kw or "IfNoneMatch" in kw:
            raise botocore.exceptions.ClientError(
                {"Error": {"Code": "NotImplemented"},
                 "ResponseMetadata": {"HTTPStatusCode": 501}}, "PutObject")


class StubDownload:
    """`download_file` the way s3transfer drives it.

    Bytes are handed to `Callback` as they land, and a part that has to be
    re-read rewinds what it already reported with a negative delta — the
    property the byte cap leans on, so the stub has to have it too. The
    destination only appears once the whole body is through, which is what
    makes an unbounded transfer fill the disk before anything checks it.
    """

    CHUNK = 256 << 10

    def __init__(self, body: bytes, retry_after: int | None = None):
        self.body, self.retry_after, self.transferred = body, retry_after, 0
        self.config = None

    def download_file(self, Bucket, Key, Filename, Callback=None, **kw):
        self.config = kw.get("Config")
        buf, rewound = bytearray(), False
        while len(buf) < len(self.body):
            chunk = self.body[len(buf):len(buf) + self.CHUNK]
            buf += chunk
            self.transferred += len(chunk)
            if Callback:
                Callback(len(chunk))
                if self.retry_after is not None and not rewound \
                        and len(buf) >= self.retry_after:
                    Callback(-len(buf))       # the part is re-read from zero
                    buf, rewound = bytearray(), True
        Path(Filename).write_bytes(self.body)


class TestPrivateDownload(Base):
    """The S3 read path, which used to enforce no byte cap whatsoever."""

    def remote(self, stub):
        with unittest.mock.patch.object(
                transport._creds, "load",
                lambda root, bucket: {"R2_BUCKET_NAME": "b"}), \
                unittest.mock.patch.object(transport._creds, "client",
                                           lambda c: stub):
            return Remote(self.cfg, need_write=True)

    def dest(self):
        return self.root / "data/cache/bundle.tar.zst"

    def test_an_object_longer_than_promised_never_reaches_the_disk(self):
        stub = StubDownload(b"x" * (4 << 20))
        dest = self.dest()
        with self.assertRaises(Oversized) as e:
            self.remote(stub).download("v1/bundle.tar.zst", dest, 10)
        self.assertIn("10 bytes promised", str(e.exception))
        self.assertFalse(dest.exists())
        self.assertFalse(dest.with_suffix(dest.suffix + ".part").exists())
        # Aborted part-way, not after the whole object was on disk.
        self.assertLess(stub.transferred, len(stub.body))

    def test_an_object_exactly_the_promised_size_is_not_refused(self):
        # `archive_pull` promises `archive_size` to the byte, so an off-by-one
        # here refuses every bundle there is.
        body = b"y" * (1 << 20)
        stub = StubDownload(body)
        dest = self.dest()
        self.remote(stub).download("v1/bundle.tar.zst", dest, len(body))
        self.assertEqual(dest.read_bytes(), body)

    def test_a_retried_part_does_not_count_its_bytes_twice(self):
        # s3transfer reports a re-read part as a negative delta rather than
        # starting the count again; without that a dropped connection would
        # push an exactly-promised object over its own cap.
        body = b"z" * (1 << 20)
        stub = StubDownload(body, retry_after=512 << 10)
        dest = self.dest()
        self.remote(stub).download("v1/bundle.tar.zst", dest, len(body))
        self.assertEqual(dest.read_bytes(), body)
        self.assertGreater(stub.transferred, len(body))    # it really retried

    def test_downloads_use_the_projects_transfer_settings(self):
        # Reads went out on boto3's defaults while writes used the tuned
        # config, against the same bucket. Ten-way concurrency also put more
        # bytes in flight before the cap above could abort, and gave each of
        # `download_many`'s eight workers ten more threads to spawn.
        stub = StubDownload(b"q" * 1024)
        self.remote(stub).download("v1/bundle.tar.zst", self.dest(), 1 << 20)
        want = creds.transfer_config()
        self.assertIsNotNone(stub.config)
        self.assertEqual(
            (stub.config.multipart_threshold, stub.config.multipart_chunksize,
             stub.config.max_concurrency),
            (want.multipart_threshold, want.multipart_chunksize,
             want.max_concurrency))

    def test_no_promised_size_downloads_without_a_callback(self):
        stub = StubDownload(b"w" * 1024)
        dest = self.dest()
        self.remote(stub).download("v1/bundle.tar.zst", dest)
        self.assertEqual(dest.read_bytes(), stub.body)


class TestConditionalWrite(Base):
    """The real `put_bytes`. The fake bucket answers conflicts itself, so the
    412 handling has never been exercised through it."""

    BODY = b'{"artifacts": {}}'

    def remote(self, stub):
        with unittest.mock.patch.object(
                transport._creds, "load",
                lambda root, bucket: {"R2_BUCKET_NAME": "b"}), \
                unittest.mock.patch.object(transport._creds, "client",
                                           lambda c: stub):
            return Remote(self.cfg, need_write=True)

    def test_a_412_over_our_own_bytes_is_not_a_conflict(self):
        # The write landed and the response was lost; botocore's retry then
        # tripped over the first attempt's own object. Nobody else published.
        stub = StubS3(self.BODY)
        self.remote(stub).put_bytes("m.json", self.BODY, "application/json",
                                    if_match="v1")
        self.assertEqual(stub.puts, 1)

    def test_a_412_over_someone_elses_bytes_is_still_a_conflict(self):
        stub = StubS3(b'{"artifacts": {"theirs": {}}}')
        with self.assertRaises(Conflict) as e:
            self.remote(stub).put_bytes("m.json", self.BODY,
                                        "application/json", if_match="v1")
        self.assertIn("someone else published", str(e.exception))

    def test_an_unreadable_manifest_after_a_412_is_still_a_conflict(self):
        stub = StubS3(None, readable=False)
        with self.assertRaises(Conflict):
            self.remote(stub).put_bytes("m.json", self.BODY,
                                        "application/json", if_absent=True)

    def test_unsupported_preconditions_never_retry_unconditionally(self):
        stub = StubUnsupportedConditional()
        with self.assertRaises(SystemExit) as e:
            self.remote(stub).put_bytes("m.json", self.BODY,
                                        "application/json", if_match="v1")
        self.assertIn("was not written", str(e.exception))
        self.assertIn("could overwrite another maintainer", str(e.exception))
        self.assertEqual(len(stub.calls), 1)
        self.assertEqual(stub.calls[0]["IfMatch"], '"v1"')


# --- end to end, through the real transport ---------------------------------

class TestRoundTripOverHttpLikeReads(Base):
    """Push with the fake bucket, then pull with the real `Remote`.

    A `file://` base drives the same code path a public HTTPS mirror does —
    the URL builder, the streaming download, the thread pool, the staging and
    the verification — without a server or a network.
    """

    def setUp(self):
        super().setUp()
        self.bucket = Path(tempfile.mkdtemp(prefix="litmo-bucket-"))
        self.addCleanup(shutil.rmtree, self.bucket, ignore_errors=True)

    def publish(self):
        """Run a push into the fake bucket, then lay it out as files."""
        fake, man = Fake(), Manifest({})
        ctx = Ctx(self.cfg, fake, man)
        for art in self.cfg.artifacts:
            kinds.push(ctx, art)
        fake.put_bytes(self.cfg.manifest_key, man.dump(), "application/json")
        for key, body in fake.objects.items():
            dest = self.bucket / key
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(body)

    def reader(self):
        cfg = dataclasses.replace(self.cfg, base=self.bucket.as_uri())
        remote = Remote(cfg)
        return Ctx(cfg, remote, Manifest.load(remote, cfg))

    def test_a_whole_repository_round_trips(self):
        for i in range(4):
            self.write(f"data/cache/{i}.json", f'{{"n": {i}}}')
        self.write("out/summary.csv", "a,b\n1,2\n")
        self.write("out/sub/detail.json", '{"detail": true}')
        before = tree_hash(self.root / "data/cache"), tree_hash(self.root / "out")

        self.publish()
        shutil.rmtree(self.root / "data/cache")
        shutil.rmtree(self.root / "out")

        ctx = self.reader()
        for art in ctx.cfg.artifacts:
            kw = {"workers": 4} if art.kind == "mirror" else {}
            kinds.pull(ctx, art, **kw)

        self.assertEqual(
            (tree_hash(self.root / "data/cache"), tree_hash(self.root / "out")),
            before)
        for art in ctx.cfg.artifacts:
            self.assertEqual(kinds.status(ctx, art).verdict, "in sync")

    def test_a_key_with_awkward_characters_survives_the_url(self):
        self.write("out/a file #1.csv", "x,y\n")
        self.publish()
        (self.root / "out/a file #1.csv").unlink()
        ctx = self.reader()
        kinds.mirror_pull(ctx, {a.name: a for a in ctx.cfg.artifacts}["out"])
        self.assertEqual((self.root / "out/a file #1.csv").read_text(), "x,y\n")

    def test_a_tampered_object_is_caught_and_nothing_is_installed(self):
        self.write("out/a.csv", "trustworthy\n")
        self.publish()
        (self.bucket / "out/a.csv").write_text("tampered with\n")
        (self.root / "out/a.csv").write_text("stale but mine\n")

        ctx = self.reader()
        with self.assertRaises(SystemExit):
            kinds.mirror_pull(ctx, {a.name: a for a in ctx.cfg.artifacts}["out"])
        self.assertEqual((self.root / "out/a.csv").read_text(), "stale but mine\n")


# --- the command line -------------------------------------------------------

class TestCli(Base):
    def run_cli(self, argv, remote=None, cfg=None):
        remote = remote or Fake()
        cfg = cfg or self.cfg

        class _Ctx(cli.Ctx):
            @property
            def remote(_self):
                _self._remote = remote
                return remote

        original = cli.config.load, cli.Ctx
        cli.config.load, cli.Ctx = (lambda *a, **k: cfg), _Ctx
        try:
            return cli.main(argv), remote
        finally:
            cli.config.load, cli.Ctx = original

    def test_doctor_comes_back_from_a_quarto_that_never_answers(self):
        # The one command whose whole job is diagnosing a broken toolchain,
        # on exactly the machine it exists for.
        def hangs(cmd, **kw):
            raise cli.subprocess.TimeoutExpired(cmd, kw["timeout"])

        out = io.StringIO()
        with unittest.mock.patch.object(cli.shutil, "which",
                                        lambda name: f"/usr/bin/{name}"), \
                unittest.mock.patch.object(cli.subprocess, "run", hangs), \
                contextlib.redirect_stdout(out):
            rc, _ = self.run_cli(["doctor"])
        self.assertIn("did not answer", out.getvalue())
        self.assertEqual(rc, 1)

    def test_doctor_fails_a_quarto_that_is_on_path_but_does_not_run(self):
        # `shutil.which` finding it is not the same as it working. A missing
        # shared library, a broken wrapper or a half-finished install answers
        # `--version` with a non-zero exit and nothing on stdout; doctor
        # printed `quarto ` with an empty version, marked it ok and exited 0.
        # Reproduced end to end with a stub on PATH exiting 127.
        broken = subprocess.CompletedProcess(
            ["quarto", "--version"], 127, stdout="",
            stderr="quarto: error while loading shared libraries: "
                   "libcrypto.so.3: cannot open shared object file\n")
        out = io.StringIO()
        with unittest.mock.patch.object(cli.shutil, "which",
                                        lambda name: f"/usr/bin/{name}"), \
                unittest.mock.patch.object(cli.subprocess, "run",
                                           lambda *a, **k: broken), \
                contextlib.redirect_stdout(out):
            rc, _ = self.run_cli(["doctor"])
        self.assertEqual(rc, 1)
        self.assertIn("exited 127", out.getvalue())
        self.assertIn("libcrypto.so.3", out.getvalue())   # why, not just that

    def test_doctor_still_passes_a_quarto_that_answers(self):
        working = subprocess.CompletedProcess(
            ["quarto", "--version"], 0, stdout="1.5.57\n", stderr="")
        self.write("common.mk", cli.mk_text())    # so nothing else fails it
        out = io.StringIO()
        with unittest.mock.patch.object(cli.shutil, "which",
                                        lambda name: f"/usr/bin/{name}"), \
                unittest.mock.patch.object(cli.subprocess, "run",
                                           lambda *a, **k: working), \
                contextlib.redirect_stdout(out):
            rc, _ = self.run_cli(["doctor"])
        self.assertEqual(rc, 0)
        self.assertIn("quarto 1.5.57", out.getvalue())

    def test_workers_must_be_at_least_one(self):
        with self.assertRaises(SystemExit) as e:
            self.run_cli(["pull", "-w", "0"])
        self.assertEqual(e.exception.code, 2)

    def test_push_writes_the_manifest_once_at_the_end(self):
        self.write("out/a.csv", "hello")
        rc, remote = self.run_cli(["push", "out"])
        self.assertEqual(rc, 0)
        self.assertIn("manifest.json", remote.objects)
        self.assertIn(b"out/a.csv", remote.objects["manifest.json"])

    def test_push_with_nothing_to_say_writes_no_manifest(self):
        self.write("out/a.csv", "hello")
        _, remote = self.run_cli(["push", "out"])
        before = remote.objects["manifest.json"]
        self.run_cli(["push", "out"], remote=remote)
        self.assertEqual(remote.objects["manifest.json"], before)

    def test_a_concurrent_publisher_is_refused_not_overwritten(self):
        class Racing(Fake):
            """A bucket where someone else publishes mid-push."""

            def upload(self, src, key, content_type):
                super().upload(src, key, content_type)
                self._stamp("manifest.json")

        self.write("out/a.csv", "hello")
        _, remote = self.run_cli(["push", "out"], remote=Racing())
        was = remote.objects["manifest.json"]

        self.write("out/a.csv", "changed")
        with self.assertRaises(Conflict):
            self.run_cli(["push", "out"], remote=remote)
        self.assertEqual(remote.objects["manifest.json"], was)

    def test_an_interrupted_push_still_commits_the_manifest(self):
        self.write("out/a.csv", "one")
        self.write("out/b.csv", "two")
        remote = Fake()
        remote.fail_upload_after = 1
        with self.assertRaises(OSError):
            self.run_cli(["push", "out"], remote=remote)
        self.assertIn("manifest.json", remote.objects)
        published = set(remote.objects) - {"manifest.json"}
        for key in published:
            self.assertIn(key.encode(), remote.objects["manifest.json"])

    def test_an_empty_bucket_does_not_block_a_fetch_artifact(self):
        cfg = self.reload(SYNC_TOML + FETCH_TOML)
        calls = []
        original = kinds.VERBS["fetch"]
        kinds.VERBS["fetch"] = (original[0],
                                lambda ctx, art, **kw: calls.append(art.name),
                                original[2])
        self.addCleanup(kinds.VERBS.__setitem__, "fetch", original)

        rc, _ = self.run_cli(["pull"], cfg=cfg)
        self.assertEqual(rc, 1)                 # the bucket really is empty
        self.assertEqual(calls, ["positions"])  # and the input still arrived


# --- the shared makefile ----------------------------------------------------

class TestSharedMakefile(Base):
    """`litmo mk` is the whole road the packaged file travels.

    Every consumer bootstraps with `common.mk: ; uv run litmo mk > $@`, so
    this one command carries the file across the wheel boundary, and `doctor`
    is what tells a vendored copy it has gone stale. CI proves common.mk is
    *in* the wheel; these prove the code can still read it back out.
    """

    packaged = Path(cli.__file__).parent / "data" / "common.mk"

    def mk(self) -> str:
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            rc = cli.main(["mk"])
        self.assertEqual(rc, 0)
        return out.getvalue()

    def test_readme_makefile_bootstraps_and_names_real_visible_targets(self):
        readme = Path(__file__).resolve().parents[1].joinpath("README.md").read_text()
        sample = readme.split("```make\n", 1)[1].split("```", 1)[0]
        self.assertIn("common.mk: ; uv run litmo mk > $@", sample)
        self.assertIn("REPRODUCE := env sync build render", sample)
        self.assertNotIn("REPRODUCE := env fetch", sample)
        self.assertIn("build:  ##", sample)
        self.assertIn("check:  ##", sample)

    def test_central_docs_match_the_generated_document_convention(self):
        readme = Path(__file__).resolve().parents[1].joinpath("README.md").read_text()
        package_docs = __import__("litmo").__doc__
        generated = self.packaged.read_text(encoding="utf-8")
        for docs in (readme, package_docs, generated):
            self.assertRegex(docs, r"(?:documents do no|no document does) arithmetic")
        for docs in (readme, package_docs):
            docs = " ".join(docs.split())
            self.assertIn("may read stored artifacts", docs)
            self.assertIn("or call", docs)
            self.assertNotIn("documents only read", docs)

    def test_mk_prints_the_packaged_file_verbatim(self):
        # The bytes a consumer's Makefile redirects into its own common.mk.
        self.assertEqual(self.mk(),
                         self.packaged.read_text(encoding="utf-8"))

    def test_mk_prints_a_usable_makefile_not_an_empty_one(self):
        # Equality with the packaged file says nothing if both are empty, and
        # a `common.mk:` rule that succeeds silently leaves a 0-byte file that
        # `-include` then loads happily on every run after.
        text = self.mk()
        for verb in ("sync:", "render:", "publish:", "doctor:", "help:"):
            self.assertIn(f"\n{verb}", text)

    def test_mk_reads_the_file_through_the_package_not_the_cwd(self):
        # It is run from the consumer's checkout, where no litmo/data/ exists.
        here = os.getcwd()
        os.chdir(self.root)
        self.addCleanup(os.chdir, here)
        self.assertIn("GENERATED by `litmo mk`", self.mk())

    def _make_sandbox(self):
        """A consumer-shaped checkout: the packaged common.mk, a Makefile that
        includes it, and stub `litmo`/`quarto` that log when they run."""
        d = self.root / "consumer"
        (d / "bin").mkdir(parents=True)
        (d / "reports").mkdir()
        (d / "reports/r.qmd").write_text("")
        (d / "common.mk").write_text(self.packaged.read_text(encoding="utf-8"))
        (d / "Makefile").write_text("include common.mk\nbuild:\n"
                                    "\t@echo BUILD >> $(LOG)\ncheck:\n\t@true\n")
        log = d / "log"
        for name, body in (("litmo", "echo SYNC_STARTED >> %s\nsleep 1\n"
                                     "echo SYNC_COMPLETE >> %s\n"),
                           ("quarto", "echo RENDER_STARTED >> %s\n"
                                      "echo RENDER_COMPLETE >> %s\n")):
            p = d / "bin" / name
            p.write_text("#!/bin/sh\n" + body % (log, log))
            p.chmod(0o755)
        return d, log

    def _run_make(self, d, *args, expect=0):
        env = dict(os.environ, PATH=f"{d / 'bin'}:{os.environ['PATH']}")
        r = subprocess.run(["make", "-j4", "UV=", f"LOG={d / 'log'}", *args],
                           cwd=d, env=env, capture_output=True, text=True,
                           timeout=120)
        self.assertEqual(r.returncode, expect, r.stdout + r.stderr)
        if expect or not (d / "log").exists():
            return r
        return (d / "log").read_text().split()

    @unittest.skipUnless(GNU_MAKE, "GNU make is not on this PATH")
    def test_a_failed_mk_update_leaves_the_working_common_mk_alone(self):
        # `litmo mk > common.mk` truncates the file before litmo runs, so a
        # missing package or a bad install left a 0-byte common.mk — which
        # `include` loads perfectly happily, taking every shared target with
        # it. Nothing after that point could tell you why.
        d, _ = self._make_sandbox()
        before = (d / "common.mk").read_text()
        (d / "bin/litmo").write_text(
            '#!/bin/sh\n[ "$1" = mk ] && { echo "boom" >&2; exit 1; }\n')
        (d / "bin/litmo").chmod(0o755)

        self._run_make(d, "mk-update", expect=2)
        self.assertEqual((d / "common.mk").read_text(), before)
        self.assertFalse((d / "common.mk.new").exists())

        (d / "bin/litmo").write_text(
            '#!/bin/sh\n[ "$1" = mk ] && { echo "# regenerated"; exit 0; }\n')
        (d / "bin/litmo").chmod(0o755)
        self._run_make(d, "mk-update", expect=0)
        self.assertEqual((d / "common.mk").read_text(), "# regenerated\n")
        self.assertFalse((d / "common.mk.new").exists())

    @unittest.skipUnless(GNU_MAKE, "GNU make is not on this PATH")
    def test_the_ordered_targets_stay_ordered_under_parallel_make(self):
        # `default: sync render` and `reproduce: $(REPRODUCE)` named
        # prerequisites, and prerequisites are not an order: `make -j` started
        # the render while the pull was still moving files into out/, and the
        # report it produced from whatever had landed exited 0 with nothing to
        # say it had happened.
        d, _ = self._make_sandbox()
        self.assertEqual(self._run_make(d, "default"),
                         ["SYNC_STARTED", "SYNC_COMPLETE",
                          "RENDER_STARTED", "RENDER_COMPLETE"])

        (d / "log").unlink()
        self.assertEqual(
            self._run_make(d, "REPRODUCE=sync build render", "reproduce"),
            ["SYNC_STARTED", "SYNC_COMPLETE", "BUILD",
             "RENDER_STARTED", "RENDER_COMPLETE"])

    def doctor_says(self) -> str:
        """The one `doctor` line about the vendored common.mk."""
        out = io.StringIO()
        original = cli.config.load
        cli.config.load = lambda *a, **k: self.cfg
        try:
            # No quarto and no uv on this PATH: those lines fail, and looking
            # only at ours keeps the check off the machine's toolchain.
            with unittest.mock.patch.object(cli.shutil, "which",
                                            lambda name: None), \
                    contextlib.redirect_stdout(out):
                cli.main(["doctor"])
        finally:
            cli.config.load = original
        return next(ln for ln in out.getvalue().splitlines()
                    if "common.mk" in ln)

    def test_doctor_passes_a_vendored_copy_that_matches(self):
        self.write("common.mk", self.packaged.read_text(encoding="utf-8"))
        self.assertRegex(self.doctor_says(), r"^\s*ok\b")

    def test_doctor_fails_a_vendored_copy_that_has_drifted(self):
        self.write("common.mk",
                   self.packaged.read_text(encoding="utf-8") + "\nstale:\n")
        line = self.doctor_says()
        self.assertRegex(line, r"^\s*FAIL\b")
        self.assertIn("make mk-update", line)

    def test_doctor_fails_a_checkout_with_no_vendored_copy(self):
        line = self.doctor_says()
        self.assertRegex(line, r"^\s*FAIL\b")
        self.assertIn("litmo mk > common.mk", line)


if __name__ == "__main__":
    unittest.main()
