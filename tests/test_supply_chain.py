"""Guards on what CI and a build are allowed to resolve for themselves.

Nothing here imports litmo. These are properties of the repository's own
manifests, and each one is a defect that comes back silently: the pin is a
line of YAML or TOML that a later edit can drop without any test going red,
and the consequence only shows up as a version nobody chose.

    uv run python -m unittest discover -s tests
"""

from __future__ import annotations

import re
import tomllib
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CI = ROOT / ".github" / "workflows" / "ci.yml"
PYPROJECT = ROOT / "pyproject.toml"

# `uvx <tool>` resolves a release independent of the lockfile, and `uv tool
# run` is the same command spelled out — uvx is an alias for it — so matching
# only the alias leaves the spelled-out form free to walk past the guard.
RESOLVES_ITSELF = re.compile(r"(^|[;&|\s])(uvx|uv\s+tool\s+run)(\s|$)")


def version_spec(requirement: str) -> str:
    """The PEP 508 requirement with its environment marker removed.

    The checks below ask what a version specifier says, and a marker is not
    one. `boto3>=1.34; python_version < "3.14"` carries no upper bound at
    all, but a guard that scans the whole string for `<` finds one in the
    marker and passes — asserting a property the manifest does not have.
    The marker is whatever follows the first `;`, which cannot appear in a
    specifier.
    """
    return requirement.split(";", 1)[0]


class TestCiUsesTheLock(unittest.TestCase):
    """CI must run the versions `uv.lock` names, not ones it resolves itself."""

    def test_no_step_runs_uvx(self):
        # A tool upgraded upstream would change what CI does before anyone
        # reviews it. `uv run` takes the locked version instead.
        offenders = [
            line.strip()
            for line in CI.read_text().splitlines()
            if RESOLVES_ITSELF.search(line)
        ]
        self.assertEqual(offenders, [], "CI runs a tool outside the lockfile")

    def test_ruff_is_linted_through_the_lock(self):
        self.assertRegex(CI.read_text(), r"uv run --locked ruff check\b")


class TestBuildBackendIsPinned(unittest.TestCase):
    """`build-system.requires` is the only thing that picks the backend.

    It is resolved in an isolated environment that `uv.lock` never constrains,
    so an unpinned entry means a build can use a backend nobody chose.
    """

    def test_every_build_requirement_is_exact(self):
        requires = tomllib.loads(PYPROJECT.read_text())["build-system"]["requires"]
        self.assertTrue(requires)
        floating = [r for r in requires if "==" not in version_spec(r)]
        self.assertEqual(floating, [], "build backend resolves itself at build time")

    def test_the_whole_backend_closure_is_named(self):
        # Pinning the backend alone is not enough: Hatchling's own five
        # dependencies float, and `uv build` resolves them from the index at
        # build time. Naming them here is what makes the build environment
        # fully chosen, so this asserts the closure is still the recorded one.
        #
        # It fails on a Hatchling bump that adds or drops a dependency, which
        # is the way the floating resolve comes back silently. Re-derive with
        #     uv build -v 2>&1 | grep 'Adding transitive dependency'
        # then update this set and the pins together.
        requires = tomllib.loads(PYPROJECT.read_text())["build-system"]["requires"]
        names = {re.split(r"[=<>!~\[; ]", r, maxsplit=1)[0].lower() for r in requires}
        self.assertEqual(
            names,
            {
                "hatchling",
                "packaging",
                "pathspec",
                "pluggy",
                "tomlkit",
                "trove-classifiers",
            },
            "the build environment has a package nobody pinned",
        )


class TestPublishedRangesAreBounded(unittest.TestCase):
    """The lock protects this checkout; these ranges are what a consumer reads.

    Both optional dependencies reach substantive APIs, so an unbounded range
    lets an untested major arrive on a consumer's first resolution.
    """

    def test_every_published_requirement_has_an_upper_bound(self):
        project = tomllib.loads(PYPROJECT.read_text())["project"]
        published = list(project.get("dependencies", []))
        for extra in project.get("optional-dependencies", {}).values():
            published.extend(extra)
        self.assertTrue(published)
        unbounded = [r for r in published if "<" not in version_spec(r)]
        self.assertEqual(unbounded, [], "a future major can arrive unreviewed")


class TestTheGuardsThemselves(unittest.TestCase):
    """The checks above are string scans, and a string scan can pass wrongly.

    Each case here is a manifest or workflow edit that leaves the guarded
    property false while the guard goes green — the exact way these checks
    stop working without anything going red.
    """

    def test_a_marker_does_not_supply_the_upper_bound(self):
        # `<` inside `python_version < "3.14"` is not an upper bound on boto3.
        self.assertNotIn("<", version_spec('boto3>=1.34; python_version < "3.14"'))
        self.assertIn("<", version_spec("boto3>=1.34,<2"))

    def test_a_marker_does_not_make_a_requirement_exact(self):
        # `==` inside `python_version == "3.13"` does not pin hatchling.
        floating = 'hatchling>=1.32; python_version == "3.13"'
        self.assertNotIn("==", version_spec(floating))
        self.assertIn("==", version_spec("hatchling==1.32.0"))

    def test_the_spelled_out_form_of_uvx_is_caught(self):
        # uvx is an alias for `uv tool run`; both resolve outside the lock.
        # This drives the pattern the check above uses, not a copy of it.
        for line in ("      - run: uv tool run mypy litmo", "      - run: uvx mypy"):
            with self.subTest(line=line):
                self.assertRegex(line, RESOLVES_ITSELF)
        for line in (
            "      - run: uv run --locked ruff check litmo tests",
            "      - run: uv lock --check",
        ):
            with self.subTest(line=line):
                self.assertNotRegex(line, RESOLVES_ITSELF)


if __name__ == "__main__":
    unittest.main()
