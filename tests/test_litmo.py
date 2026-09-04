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
import hashlib
import http.client
import io
import json
import os
import shutil
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

        def unchanged(path, e):
            if path.name == "a.csv":
                raise RuntimeError("stat blew up after the PUT")
            return real_unchanged(path, e)

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

        def interrupted(path, e):
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
        self.write("data/cache/1.json", "{}")
        remote, man = Fake(), Manifest({})
        ctx = Ctx(self.cfg, remote, man)
        kinds.archive_push(ctx, self.art())
        self.write("data/cache/mine.json", "{}")
        kinds.archive_pull(ctx, self.art())
        self.assertTrue((self.root / "data/cache/mine.json").exists())
        self.assertTrue((self.root / "data/cache/1.json").exists())

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


class TestRemote(Base):
    def served(self):
        """A `file://` base, so the public read path runs with no server."""
        d = Path(tempfile.mkdtemp(prefix="litmo-served-"))
        self.addCleanup(shutil.rmtree, d, ignore_errors=True)
        return d, Remote(dataclasses.replace(self.cfg, base=d.as_uri()))

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
