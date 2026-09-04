# litmo

Shared plumbing for literate-analysis repositories, so that they answer to the
same commands, keep their data in the same shape, and move it to and from
object storage with one implementation instead of three.

It exists for a small, specific reason: three repositories had three different
ways to start (a `Makefile`, a `run_*.py`, and a README you copy commands out
of) and three different sync mechanisms, and remembering which was which cost
more than the code was worth.

## The convention

One rule, and the rest follows from wanting it to be checkable:

> **Analysis modules do all the computing; documents do no arithmetic.**

So the layout is:

```
Makefile  common.mk  sync.toml  pyproject.toml  uv.lock  _quarto.yml
reports/     *.qmd sources; *.md tracked, *.html not
scripts/     stage entry points        <pkg>/  importable library
data/        inputs      — git-ignored, synced from a bucket
out/         artifacts   — git-ignored, synced from a bucket
.litmo/      litmo's own state and pull staging — git-ignore it
```

and the commands are:

```
make            sync + render — the "I just cloned this" path
make env        install from the lockfile
make sync       pull data/ and out/ from object storage
make build      run the pipeline: data/ -> out/
make render     reports/*.qmd -> the formats this repo publishes
make preview    live preview of one report
make check      tests and validators
make publish    push data/ and out/ (maintainer only)
make reproduce  everything, from an empty checkout
make clean      remove derived files
```

Those commands are the interface litmo gives its consuming analysis
repositories; they are not this package's development commands.

A document may read stored artifacts from `out/`, or call analysis modules and
format what they return. The latter recomputes from inputs on every render.
Either way `freeze: false` is safe: the document owns no computation or cached
number that can drift from the analysis code.

## Using it

```sh
uv add "litmo[all] @ git+https://github.com/evmo/litmo"
uv run litmo mk > common.mk        # then commit it
```

A repository's `Makefile` is then the small part that is genuinely its own:

```make
FORMATS   := gfm html
REPRODUCE := env sync build render

-include common.mk
common.mk: ; uv run litmo mk > $@

build:  ## run the pipeline
	uv run python -m mypkg.pipeline all

check:  ## run tests and validators
	uv run pytest
```

`make help` lists every target; `make doctor` checks the toolchain, the
config and the credentials.

The extras track what a repo actually does: `litmo` alone is stdlib-only and
enough to pull public files, `litmo[s3]` adds publishing, `litmo[archive]`
adds the bundled kind, `litmo[all]` is both.

## Sync

Each repository declares what it publishes in `sync.toml`:

```toml
[remote]
base = "https://artifacts.example.org"  # optional: public reads
manifest = "manifest.json"              # optional object key; this is default

[artifact.cache]
kind = "archive"
path = "data/cache"
key  = "v1/data-cache.tar.zst"
what = "raw API responses, keyed by hash of their parameters"
```

Every artifact kind also accepts `manual = true`. A bare `litmo pull` or
`litmo push` skips a manual artifact; naming it explicitly includes it, while
`litmo status` always reports it. This is useful for a refetchable input that
is tracked in Git and should only be overwritten deliberately.

Three kinds, chosen by what the files are rather than by taste:

| kind | for | identity | credentials |
|---|---|---|---|
| `archive` | a tree of thousands of small files | hash of the file tree | to read a private bucket; always to write |
| `mirror` | a modest number of individually useful files | sha256 per file | none to read a public one; always to write |
| `fetch` | inputs published by something outside the repo | the server's ETag | none — pull-only |

`archive` identity is the hash of the *tree* — every relative path and the
sha256 of every file — and never of the bundle, because zstd is not
bit-reproducible across versions and hashing the bundle would make an unchanged
corpus look changed on a different machine.

One `manifest.json` per bucket describes everything in it. Two older manifest
shapes are read transparently, so pointing litmo at an existing bucket does
not mean re-uploading it.

Everything a reader trusts comes from that one object, so it is not taken on
faith. Its shape and every digest in it are checked before anything is acted
on, and no path in it can name a file outside the artifact that claims it.
Both pulls stage: a bundle or a set of files is downloaded in full and checked
against the manifest before a single byte of the working tree changes. A
download or verification failure, or a known local layout conflict, therefore
leaves the checkout untouched. An archive then installs with a whole-tree swap;
a mirror has to install its independently named files one at a time, so an
unexpected operating-system error during that final step can leave earlier
files updated. Correct the error and rerun the pull.

Publishing is not a transaction, and litmo does not claim to be one. Objects
are stored under their own names — that is what makes a public bucket
browsable — so a push that dies partway has already changed it. What litmo
does promise is that the manifest never names an object that was not uploaded,
that it is then written to describe what actually landed, and that it goes up
with an `If-Match` on the copy that push read, so two maintainers publishing at
once get a refusal rather than a silently lost entry.

```
litmo pull [name...]      bucket -> here      (make sync)
litmo push [name...]      here -> bucket      (make publish)
litmo status [name...]    compare; non-zero if they differ
litmo mk                  print common.mk
litmo doctor              check toolchain, config, credentials
```

Credentials come from `.r2` at the repository root (git-ignored) or from the
environment, which wins — a laptop supplies them without an export, CI without
a file. If `.r2` holds a secret and other users on the machine can read it,
loading is refused rather than warned about: it is a live write credential for
a bucket readers trust. Start from the shipped template:

```sh
cp .r2.example .r2
chmod 600 .r2
```

The canonical file and environment names are `R2_ACCOUNT_ID`,
`R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, and `R2_BUCKET_NAME`. The legacy
name `R2_BUCKET` is also accepted from the environment as an alias for
`R2_BUCKET_NAME`.

## Contributing to litmo

This repository itself has no Makefile. From a fresh clone, install
[uv](https://docs.astral.sh/uv/), then create the locked development environment
and run the same checks as CI:

```sh
uv sync --all-extras
uv run python -m unittest discover -s tests -v
uv run --locked ruff check litmo tests
uv lock --check
uv build
```

The project supports Python 3.12, 3.13, and 3.14; CI runs the suite under all
three. A local run uses the interpreter selected by uv for this checkout.

## License

MIT.
