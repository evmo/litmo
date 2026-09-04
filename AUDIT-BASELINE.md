# AUDIT-BASELINE.md

Findings we have seen and consciously accepted. Audit prompts read this file
first and skip anything listed here. Keep it short; if an entry stops being
true, delete it.

## Not a defect

- **`litmo/data/common.mk:110` — the `clean` guard does *not* miss `~`.**
  Reported by audit-failure-modes on 2026-09-01 as a bypass: a `CLEAN_EXTRA`
  of `~/scratch` supposedly passing the `case "$p" in /*|*..*)` pattern and
  then being tilde-expanded by the `rm -rf` recipe. It is not. Both lines
  expand `$(CLEAN_EXTRA)` into shell text, and the guard's own `for p in
  $(CLEAN_EXTRA)` performs tilde expansion on the word list first — so `p` is
  already `/home/<user>/scratch` when `case` sees it, and `/*` catches it.
  Verified from a real `make clean`, both with `CLEAN_EXTRA` set in a Makefile
  and passed on the command line:

      $ make clean                       # CLEAN_EXTRA ?= ~/scratch
      CLEAN_EXTRA must be repo-relative: /home/evmo/scratch
      make: *** [common.mk:110: clean] Error 1

  The forms where the tilde does survive the `for` — `"~/scratch"` quoted in
  the Makefile, or `~nosuchuser/x` — survive the `rm` identically, so they
  address a literal `./~…` inside the checkout and never leave it. Checking
  the `case` pattern against a quoted `p` in isolation is what makes this look
  real; it is not how the recipe runs. Accepted 2026-09-02.

- **`npm audit` from the litmo root is out of scope.**
  Noted by audit-deps on 2026-09-04. litmo tracks no `package.json`, npm
  lockfile or JavaScript source at all. Verified here: `npm prefix` resolves
  upward to `/home/evmo` and `npm root` to `/home/evmo/node_modules`, so an
  `npm audit` run from this checkout reports on a separate parent project.
  Any advisory it returns belongs there, not to this repository. Accepted
  2026-09-04.

- **`litmo/manifest.py:234` — `Manifest.mirror_files` is intentionally
  test-facing.** Reported by audit-deadcode on 2026-09-04 as a method with no
  production caller. It has none, and that is fine: `git grep -w mirror_files
  -- litmo/` returns the definition alone, while `tests/test_litmo.py` calls
  it 27 times to read back persisted and migrated mirror entries. Tests are
  callers, and this is a public method of the published package. Suite 189
  passed. Accepted 2026-09-04.

## Real, and deliberately not changed

- **`litmo/kinds.py:1102` — a failed mirror install still leaves a mix of two
  published generations, and there is no per-file rollback journal.**
  Reported by audit-failure-modes on 2026-09-04 as medium. Reproduced with a
  three-file `--clean` pull failing `EACCES` on its second `_move`:
  `out/a.csv` held the published bytes while `b.csv` and `c.csv` held the
  local ones. **Half of it was fixed** — the `--clean` sweep now runs after
  the install loop instead of before it, because an extra is a local-only
  file that no bucket has, so deleting it and then failing was the one
  outcome re-running the pull could not undo. `out/extra.csv` survives that
  same failure now.

  The other half of the report's fix — journal each replacement with a
  sibling backup and restore the completed moves on error — is not taken,
  because what is left costs no data. Every published file a failed install
  leaves behind is one whole generation or the other, and a mirror pull
  overwrites local edits to published files by design, so a partial install
  takes nothing a successful one would not have taken. Recovery is measured,
  not assumed: re-running the same pull downloaded `2 of 3 files` and ended
  with all three at `PUBLISHED-*`. The pull also exits non-zero, so `make
  sync` stops rather than rendering against the mix.

  Against that, the journal is new failure-path machinery on the one path
  that touches the working tree, and `_swap`'s own docstring is a record of
  how that goes wrong. It cannot park into the staging directory — an
  artifact symlinked to another disk makes that `EXDEV`, which is the lesson
  `_swap` already learned — so the parks have to be siblings inside the
  artifact, which brings three more things with it: two concurrent mirror
  pulls would then share park names and destroy each other exactly as
  `_swap` did before `_installing` (so the fix would first have to extend
  that lock to mirror installs), a killed pull would leave `.x.csv.litmo-old`
  files inside the artifact, and `_covers` would need a new exclusion or the
  next `push` would publish them. The report's own alternative — stage the
  whole tree and swap it archive-style — is refused by what a mirror is for:
  a swap deletes local extras, and a pull without `--clean` may not.

  What would change this: a consumer whose pull genuinely fails mid-install
  more than once, or a `_move` that starts failing for a reason a re-run
  cannot clear. Then the lock moves to the mirror install first, and the
  journal goes on top of it. Accepted 2026-09-04.

- **`litmo/kinds.py:1196` and `:805` — a losing or failed push really can
  leave the committed manifest describing bytes the bucket no longer holds,
  and the object keys are staying mutable.**
  Reported by audit-failure-modes on 2026-09-04 as high. Reproduced here on
  both kinds, against the real `mirror_push`/`archive_push` and the real
  manifest commit at `cli.py:128`, two publishers sharing one in-memory
  bucket. Both read `etag-2`; A uploaded `AAAA` to `out/x.csv`, B uploaded
  `BBBB` over the same key, A's conditional manifest PUT won and B's was
  refused. The published manifest then said `4677942dfa3e74b5…` (the sha of
  `AAAA`) while the object held `BBBB`. The archive half is identical:
  manifest `archive_sha256 741f7a0694ff47aa…`, bundle in the bucket
  `030abd19cdea6fff…`. The same end state is reachable with one publisher —
  any manifest PUT that fails after the objects are up.

  What keeps this off the fix list is the blast radius on the reader, which
  the report does not weigh. Both kinds fail **closed**, loudly, before
  touching anything: the mirror pull raised `out: 1 of 1 file(s) failed
  verification — nothing under out was changed` and the archive pull
  `cache: archive failed verification — data/cache was not touched`, and in
  both cases the reader's own file was still `reader-local` afterwards. No
  reader is given wrong bytes and no local copy is lost; pulls simply refuse
  until a maintainer re-runs `litmo push`, which is one command and is what
  the `Conflict` message already tells them to do.

  Against that, the report's own smallest fix — content-addressed or unique
  staging keys, with the manifest as the sole commit point — is the one
  change this repository has explicitly decided against. README:110 gives
  the reason in its own words: "Objects are stored under their own names —
  that is what makes a public bucket browsable", and the mirror kind exists
  so "a reader can fetch one file without the rest" at its published name.
  Immutable keys would change every reader's URL, need a garbage collector
  for superseded generations, and require re-uploading three live buckets.
  There is no smaller correct version of it: object-level `If-Match` would
  need per-object ETags in the manifest and a write path that is not
  `upload_file` (which cannot take a precondition at all), and it still would
  not cover a manifest PUT that fails for a network reason after the objects
  are up. README:132 already says the honest thing — "Publishing is not a
  transaction, and litmo does not claim to be one."

  What would change this: publishing moving from a maintainer at a keyboard
  to something automated that can genuinely run twice at once — a CI job on
  push, or a scheduled republish — where the window stops being two people
  who can talk to each other. Accepted 2026-09-04.

- **`litmo/paths.py` — `relative` accepts Unicode bidi overrides, and that
  is where the line is drawn.**
  Not from an audit: noticed while fixing the audit-security finding that
  control characters in a manifest path reach the terminal unescaped
  (53e08eb). The same question asked of U+202E and friends has a different
  answer, so it is recorded here rather than left for the next run to raise
  as new. Verified live: `paths.relative` accepts
  `out/annual-report\u202evsc.pdf`, which a terminal renders as
  `out/annual-reportfdp.csv` while the extension on disk is really `pdf`.

  Two things make it unlike the control characters, which were refused. A
  bidi override reorders only the characters of the name it sits in — it
  cannot erase or repaint litmo's own output the way `\x1b[2K\r` can, so
  the forged "verified" line that motivated that fix is not available here.
  And litmo executes nothing it pulls: the bytes land under the real name,
  and every downstream tool reads that name, not its rendering.

  Against that, refusing them has a real cost. Legitimate Arabic and Hebrew
  filenames carry directional marks: `out/\u200fتقرير.csv` and
  `out/\u200ereport.csv` both validate today and would keep validating
  under a marks-only carve-out, but the carve-out is the part that has to be
  got right, and getting it wrong refuses a name someone published in good
  faith. A defect that misrenders a filename is worth less than a bug that
  makes a repository unpullable.

  What would change this: litmo growing a path that *acts* on a name rather
  than storing it — opening it by extension, handing it to a shell, choosing
  a program from it. Then the rendering and the bytes disagreeing starts to
  matter. The precise cut if that day comes is U+202A–U+202E and
  U+2066–U+2069 (the embeddings, overrides and isolates), leaving U+200E and
  U+200F alone. Accepted 2026-09-02.

- **`litmo/hashing.py:24` — `tree_hash` re-reads every byte on every call,
  and there is no digest cache.**
  Reported by audit-perf on 2026-09-02 as medium, suggesting an index under
  `.litmo/` mapping relative path to `(size, mtime_ns, digest)`, git-style.
  The mechanism is exactly as described, counted live by wrapping
  `kinds.tree_hash`: one full tree read for `status`, for a no-op `pull` and
  for a no-op `push`; three for a real pull (pre-compare, staged
  verification, final message); two for a real push (pre-hash and the
  post-pack race re-read).

  What does not hold up is the scale. The report's own threshold is "visible
  around 50,000 files or 5 GB". The largest real artifact any consumer
  publishes today is owphack's `data/cache` at **2,965 files / 59 MB, which
  `tree_hash` reads in 0.102 s**; its `out` is 10 files / 20 MB in 0.012 s.
  Synthetic trees confirm the curve — 100,000 × 1 KiB is 3.10 s warm against
  a 0.67 s walk-and-stat floor, and 20 × 105 MB is 1.21 s — but nothing is
  within 16× of the knee.

  Against that 0.1 s, the index moves identity's trust from bytes to
  `(size, mtime)` in the one module whose docstring says it looks at neither,
  and its failure mode is silent and data-shaped: a stale entry makes `push`
  say "up to date" and never upload, in repos whose own `sync.toml` says the
  bucket is the only other copy.

  There is also a trap in the suggested shape. The cache must be excluded
  from `archive_pull`'s staged-tree hash at `kinds.py:421` — that hash is
  what proves the bundle is the tree the manifest describes, and `_unpack`
  restores the *publisher's* mtime onto the staged files. Measured: a file
  pulled and left alone has a staged `st_mtime` byte-identical to the local
  one (1788317538.6402206 both sides), so a cache keyed on seconds or on the
  float serves a hit and verifies nothing. Keyed on `st_mtime_ns` it misses
  by 94 ns — but only as a float-rounding artifact of the pax header, not a
  guarantee. "The index only decides which files must be re-read" is not true
  of that call site, and the report does not say so.

  What would change this: a real artifact past roughly 20,000 files or 2 GB.
  Then an index scoped to the *local* tree reads only — `archive_status`, the
  pre-push digest, the pre-pull compare — and explicitly refused at
  `kinds.py:421`, is worth writing. Accepted 2026-09-02.

  The *other* fix for the same sentence — hash the files in a pool rather
  than cache their digests — was weighed on 2026-09-04, when the mirror kind
  got exactly that (`_local_index` and the staged verification pass, both
  eight wide). It does not carry the trust-model objection above at all: it
  reads every byte, every time, and answers identically. It is declined on
  scale alone. Re-measured that day against the real archive consumer,
  owphack, best of three: `data/cache` 3,181 files / 65 MB in **0.106 s**,
  `out` 10 files / 21 MB in 0.013 s, and `data/owp.sqlite` — one 97 MB file,
  which no width can split — in 0.054 s. The largest is 5x under the 0.5 s
  this repo calls noticeable, and unlike the mirror kind the cost is not on
  the no-op path of a 2 GB tree.

  What would change *that*: an archive artifact of a few thousand files past
  roughly 1 GB, where `tree_hash` alone crosses half a second. The change is
  then small and safe — hash the sorted list in a pool, fold the digests into
  the running sha256 in that same sorted order, since the order is the hash.

- **`litmo/kinds.py:285` — `_install`'s merge loop is not worth restructuring,
  and the reason the audit gave for it is wrong.**
  audit-perf on 2026-09-02 raised this inside its "pull walks and hashes more
  times than verification needs" finding: "at 50,000 files the split was
  0.94 s in layout checks, 0.18 s in renames, and 2.46 s in the per-file
  `mkdir` and path arithmetic, over only 2,048 distinct parent directories".
  The two passes that finding was mostly about — the stray walk and the
  closing hash — are fixed. This third part is not.

  Measured here at 50,000 files over the same 2,048 parents: the `mkdir` is
  not the cost. Remembering which parents already exist cuts the calls from
  50,000 to 2,048 and the move loop from 1.29 s to 1.24 s — 0.05 s, because
  `mkdir(exist_ok=True)` on a directory that is already there is about a
  microsecond. What actually costs is the per-file layout check: `_blocked`
  is 1.72–1.89 s, and cProfile puts that in `pathlib` object construction
  (850,000 `PurePath.__init__`, 700,000 `_PathParents.__getitem__`) from
  walking `.parents` once per file rather than once per directory. Memoising
  the ancestor walk per distinct parent gives 1.23 s for a byte-identical
  answer.

  That is 0.66 s at 50,000 files. The largest artifact any consumer merges
  today is 2,965 files, where it is under 0.05 s, and unlike the two passes
  that were removed this one is not a deletion — it splits `_blocked` in two
  and puts a cache in its caller, on the path that decides whether a pull is
  allowed to touch the working tree at all.

  What would change this: a real artifact past roughly 50,000 files that is
  pulled without `--clean`, which is the only path this loop runs on.
  Accepted 2026-09-02.

- **`litmo/remote.py:180` — the public read path really does open a
  connection per object, and it is staying that way for now.**
  Reported by audit-perf on 2026-09-02 as low. Reproduced: a `download_many`
  of 40 objects at 8 workers against a local HTTP/1.1 server that offers
  keep-alive accepted **40 TCP connections from 40 distinct client ports**.
  `urllib.request.urlopen` does not pool, exactly as reported.

  The half the audit said it could not measure — "that part could not be
  measured here and is inferred" — is measured now, against the real bucket
  a reader pulls from. Ten GETs of `nyad-currents.ultraswimming.org/
  manifest.json` (157,817 B), DNS warmed, best of three rounds:

      urlopen per object (today)   210.2 ms   rounds [213, 210, 211]
      one reused connection         85.7 ms   rounds [ 88,  86,  96]
      handshake per object         124.5 ms

  So the inference was right, and slightly conservative. The largest real
  mirror is nyad-currents at 840 files across two artifacts, where that is
  **13.1 s of a fresh `make sync`** at 8 workers — a sync that also moves
  about 1 GB of NetCDF.

  Not fixed because of what the fix costs. `urlopen` is carrying redirects,
  the `HTTPError` codes that `_transient` reads to decide what is worth
  retrying, the 404 that `get_bytes` turns into `Missing`, chunked bodies and
  proxy configuration. Hand-managed `http.client` connections in a
  `threading.local` re-derive all of that, plus discard-and-reconnect on a
  socket that died between requests — new machinery on the one path every
  credential-free reader depends on, and the path where the last two audits
  already found two real bugs (no retries at all, and a conditional PUT
  misreading its own success). Thirteen seconds once per clone does not buy
  that.

  What would change this: a mirror of a few thousand small files, where the
  handshakes cost more than the bytes do — at 5,000 files it is 78 s — or a
  profile of a real `make sync` where connect time beats transfer time.
  Accepted 2026-09-02.
