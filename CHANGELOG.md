# Changelog

## Unreleased

**Renamed from `litkit` to `litmo`.** The PyPI name `litkit` belongs to an
unrelated package, so this one could never be published under it. The
distribution, the import package and the command are all `litmo` now, and the
repository is `github.com/evmo/litmo` — GitHub redirects the old URL, but a
consumer wants its source and its vendored makefile refreshed:

    uv add 'litmo[all] @ git+https://github.com/evmo/litmo'
    make mk-update

The state directory moves from `.litkit/` to `.litmo/`; the first `litmo`
command in a checkout moves an existing one across, so no fetch state is lost.
Update the `.gitignore` entry. Buckets are untouched — no manifest has ever
carried the tool's name, so nothing has to be re-pushed or re-pulled.

Integrity work, prompted by an audit. Nothing in `sync.toml` changes, and
existing buckets are read exactly as before — but a few things that used to be
tolerated are now refused, and two commands exit differently.

### Fixed

- **`doctor` could hang forever on `quarto --version`.** The subprocess had
  no timeout, so the command whose job is diagnosing a broken toolchain hung
  on exactly the machine it exists for. It waits ten seconds, then says quarto
  is wedged rather than missing, and fails.

- **A push could report its own success as someone else's.** If the
  connection dropped after R2 applied the manifest write but before the answer
  arrived, botocore's retry carried the same `If-Match` and was refused by the
  first attempt's own object — and the 412 became "someone else published",
  which in a two-maintainer repo is a sentence people act on. The manifest is
  read back before that is said; if it is what this run sent, the push
  succeeded.

- **Public HTTPS reads had no retries at all.** One connection reset or 5xx
  aborted the pull, and because staged files are deleted with the temporary
  directory, every verified byte already downloaded went with it — on a flaky
  link a large `make sync` could never finish. The read path now gets three
  attempts with a jittered backoff, for the connection-level failures only:
  a 404, a 403 or an oversized body still fails on the first answer. The write
  path was already retried ten times by botocore; this is the other half of
  that.

- **A truncated HTTP body was accepted as a complete download.** The read
  loop streams with `read1`, which answers a finished body and a connection
  that went away mid-transfer identically — with an empty chunk — so a
  response that stopped short of the `Content-Length` it declared came back
  as a successful read. Measured against a real `HTTPResponse`: a 15-byte
  object that arrived as four bytes returned cleanly after one request, and
  the mirror pull that asked for it then failed staged verification and threw
  its whole staging tree away rather than asking again. The end of the read
  now checks what is left of the server's own framing and raises
  `IncompleteRead`, which the retry path already treats as worth another
  attempt — so a truncated transfer costs the attempt, not the pull. It
  covers the manifest read too, the first thing every reader fetches.

- **A failed parallel download still ran the whole queue.** `download_many`
  raised on the first failure, but left its pool without `cancel_futures`, so
  every queued job executed before the caller saw the error. With the network
  down that is a socket timeout each — hours, for a large mirror, spent on a
  pull that had already failed. Queued jobs are cancelled; only the in-flight
  ones finish.

- **A push could publish a digest the bucket does not have.** Both kinds
  hashed before they uploaded and never looked again, so an artifact rewritten
  in between — `make build` still running under `make publish` — was described
  by a manifest that no longer matched what went up. Push exited 0 and every
  reader's `pull` failed verification until someone happened to push again.
  An archive re-hashes its tree after packing and uploads nothing if it moved;
  a mirror re-reads each file after its upload and leaves out the ones that
  did, then exits non-zero naming them.

- **A failed `pull --clean` deleted local-only files anyway.** The sweep of
  files the bucket does not have ran *before* the download, so a pull that then
  failed verification had already unlinked the only copy of each — artifact
  directories are git-ignored, and a file absent from the bucket has no other
  copy — while the error it printed said nothing under the artifact had been
  changed. The sweep now runs after verification, where the `archive` kind
  already had it.

- **Manifest paths could escape the checkout.** Entries were joined to the
  repository root and written, so `../../…` in a manifest — or a symlinked
  parent directory — could put a download outside the repo, or delete
  something outside it under `--clean`. Every path from the manifest and from
  `sync.toml` is now checked (`litmo.paths`), and manifest entries are
  contained to the artifact that claims them.
- **A failed mirror pull left corrupt files installed.** Downloads went
  straight to their destinations and were verified afterwards, so a checksum
  failure exited non-zero with the bad bytes already in place. Files are now
  staged, verified against the manifest's size *and* digest, and moved into
  place only once every file in the pull has passed.
- **A failed archive pull modified the tree and reported success.** Bundles
  were extracted into the repository and the tree hash checked afterwards,
  where a mismatch printed a note and exited 0. Extraction is now staged, the
  whole tree is hashed before anything is installed, and a mismatch is a
  non-zero exit with the working tree untouched.
- **Deleting the last file in a mirror could not be published.** `push` treated
  "no matching files" as "nothing to do", so an artifact could never reach an
  empty state. An empty directory now publishes an empty manifest entry; a
  *missing* directory is still a skip.
- **A locally edited `fetch` input read as in sync forever.** Freshness was the
  server's ETag alone. The size and digest recorded at download time are now
  checked too, so an edited file reports `DIFFERS` and is re-fetched.
- **An empty bucket blocked unrelated `fetch` artifacts.** `pull` aborted the
  whole command; it now skips what the bucket owes, still fetches the external
  inputs, and returns non-zero.
- **Orphaned manifest records were dropped on the second push.** A legacy flat
  `files` list was preserved by `dump()` but ignored by the next `load()`.
- **`.part` files survived an interrupted download**, and the local state file
  could be truncated by one. Both are now written to a sibling and renamed.
- **Object keys were concatenated into public URLs**, so a key containing a
  space, `#` or `?` addressed the wrong thing. Keys are percent-encoded.
- **An archive of a symlinked artifact directory published one dangling link
  and nothing else.** `tree_hash` reads through such a directory and `tar.add`
  did not, so the two disagreed about what had been published. Found while
  testing the containment work.
- **A merge could write outside the artifact through a symlinked parent.** A
  bundle naming `sub/x` where the local `sub` was a link out of the tree
  installed straight through it — the layout check waved a link on the way to
  a file through, and the install joins its paths rather than resolving them.
  A link below the artifact root is now a layout conflict, refused before
  anything moves. The same escape existed one level down, in the
  cross-filesystem fallback: `shutil.move` follows a link *at* the
  destination where `os.replace` replaces it, so it now lands beside the
  destination and renames.
- **A failed `pull --clean` could destroy the local copy.** The outgoing tree
  was renamed into the staging directory, which is then deleted — so anything
  the second step raised left the destination absent and nothing to put back;
  on an artifact symlinked to another filesystem it was deleted outright
  before the copy that might fail. It is now parked beside itself, where a
  rename has no boundary to cross, and rolled back if the install fails.
- **An archive key could alias a mirror's objects or the manifest itself.** A
  mirror uploads each file under its own path, so `key = "out/a.csv"` beside
  a mirror over `out` published two different digests to one object and
  committed a manifest no bucket could satisfy — every later archive pull
  failed verification. Checked when `sync.toml` loads.
- **Two things a push accepted and every pull refused.** A symlink out of the
  artifact created *after* the preflight check, and a fifo or device node
  anywhere in it: neither moves the tree hash, so the push exited 0, `status`
  said in sync, and the next push said `up to date` while no reader could
  extract the bundle. Both are refused now, the link check made against the
  bytes going into the bundle rather than a re-read of the source.
- **A `make -j` clone rendered while it was still syncing.** `default: sync
  render` names prerequisites, and prerequisites are not an order, so the
  report was built from whatever had landed — successfully. `default` and
  `reproduce` now run their stages in order.
- **A failed `make mk-update` truncated `common.mk` to nothing**, which
  `include` loads happily, taking every shared target with it. Written to a
  sibling and renamed.
- **Two archive pulls at once could leave the artifact absent.** Both swaps
  parked the outgoing tree under the same `.NAME.litmo-old`, so the second
  read the first's park as a dead run's leftover and deleted it; the first's
  rename then failed, its rollback deleted the second's tree, and there was
  no park left to restore — with the second pull having reported success.
  Installation now takes an advisory lock per artifact, waits for the other
  process, and after five minutes says so rather than hanging.
- **A push could publish a digest the bucket does not hold.** Both kinds
  re-read what they had just sent to catch a build still writing underneath
  the publish, and neither could see a rewrite that was undone before that
  re-read: a file rewritten in place while it was being uploaded, or while
  the bundle was being packed, and then put back left the object holding a
  mixture the re-read agreed with, and every reader's pull failed on it. Both
  re-reads now check the file's modification time as well as its bytes. This
  can only refuse a push, never accept one.
- **A failed `pull --clean` deleted local-only files for a pull that never
  finished.** The sweep ran before the install, so an error partway through
  left a mixture of two generations *and* the local extras already gone — the
  one thing re-running the pull cannot bring back. The sweep now runs after
  the files are in place.
- **A slow enough server could hold a pull open indefinitely.** The socket
  timeout bounds one read, not a transfer, so a byte sent just before each one
  expired kept a request alive without ever raising — the retry logic never
  got a say. Reads are now held to a floor of a kibibyte a second, measured
  over the whole transfer and applied only after the socket timeout has
  passed, so a large download over a slow link is unaffected.
- **Rate limits were retried inside the window the server asked for.** A `429`
  or `503` carrying `Retry-After: 60` got all three attempts within two
  seconds and then failed. The wait is honoured now, capped at two minutes.
- **`doctor` reported a broken quarto as `ok`.** A quarto on `PATH` whose
  `--version` exits non-zero — a missing shared library, a half-finished
  install — printed an empty version and exited 0. It now fails, with the
  first lines of what quarto said.
- **Retrying an archive pull after a killed one could delete the only local
  copy.** An install parks the outgoing tree beside itself before moving the
  new one into place, and a later run reads a leftover park as a dead run's
  rubbish and sweeps it. That is only true while the artifact directory is
  still there: a run killed between the park and the move leaves the park
  holding the *only* copy, and the retry deleted it before finding that out —
  so a retry that then failed for any reason left neither. With the
  destination absent, an existing park is now adopted as the outgoing copy
  and put back if the install fails.
- **A merge pull silently dropped published symlinks.** `pull` without
  `--clean` installed a link naming a regular file but not one naming a
  directory, and not a dangling one — and because the tree hash does not
  count links either, the pull reported the tree was a superset of the
  bucket's while two of its entries were missing. `pull --clean` was never
  affected. All three arrive now, with their targets intact.
- **Renaming an archive left the bucket describing the old layout.** An
  artifact's `tree_hash` is built from paths relative to its own directory,
  so changing its `path` or its `key` in `sync.toml` does not move the
  digest: `push` said "up to date" and never rewrote the manifest, `status`
  said "in sync" and exited 0, and every reader's pull under the new
  configuration failed with "the bundle is not the tree the manifest
  describes" — with no ordinary push able to mend it. Both verbs now compare
  where the entry says the artifact is as well as what it holds; `status`
  reads `... elsewhere` until it has been pushed.

### Added

- Manifests are validated on load: types, digest format, sizes, duplicate
  entries, artifact kinds, required archive fields, and caps on document size
  and entry count. Malformed JSON is a clear error rather than a traceback.
- The manifest is committed with an `If-Match` on the copy the run read, so two
  maintainers publishing at once get a refusal instead of a lost update. Stores
  without conditional writes fall back, with a note.
- A push that dies partway now still commits a manifest describing what
  actually reached the bucket, rather than leaving one that describes a state
  that no longer exists.
- `sync.toml` is checked for overlapping artifact paths, two archives sharing a
  key, non-`http(s)` fetch URLs, and paths that leave the repository.
- `doctor` reports read and write readiness separately: a contributor pulling
  from a public bucket is no longer told their missing credentials are a
  failure.
- Caps on archive member count and unpacked size, and on the number of bytes a
  download will accept beyond what the manifest promised.
- An absolute ceiling on one downloaded object, applied before the transfer
  starts. The size the manifest promises was the only bound on its own body,
  and the manifest is the document being distrusted: an entry claiming 10^30
  bytes streamed until the disk filled, since nothing is checked until the
  whole body is on disk. It holds for a mirror's per-file sizes too.
- A leftover staging tree from a pull that was killed outright — which can be
  the size of the artifact — is swept away by the next run.
- An unreachable bucket reads as one line rather than as a traceback.
- Tests for all of the above: 95, up from 26.
- CI across Python 3.12–3.14, `ruff`, a lockfile check, and a wheel build that
  proves `common.mk` and `py.typed` are still packaged.
- `py.typed`, project URLs and classifiers.

### Changed

- `common.mk`'s `clean` no longer word-splits `$(shell find …)` into `rm -rf`,
  and refuses a `CLEAN_EXTRA` that is absolute or contains `..`. `default` and
  `reproduce` run their stages in order under `make -j`, and `mk-update`
  cannot truncate the file it is refreshing. Repositories vendoring it should
  run `make mk-update`.
- `--workers` must be at least 1; 0 used to fail inside the thread pool.
- **Mirror commands read the local tree in parallel.** Two full passes over
  every covered file used to run one file after another, either side of a
  download that had been eight wide all along: the local index every `status`,
  `pull` and `push` builds before it can say that nothing moved, and the
  staged verification a `pull` does before it may touch the working tree.
  Both now run `--workers` wide. On the largest real mirror published from
  here — 711 files, 2.18 GB — the no-op comparison goes from 1.30 s to 0.28 s
  warm and 3.09 s to 0.84 s cold, and the verification pass from 2.25 s to
  0.55 s warm and 3.26 s to 0.96 s cold; a full 840-file, 2.28 GB pull over
  HTTPS is 5.07 s against 4.05 s. Both answers are unchanged: the index is the
  same file for file and in the same order, and a failed verification still
  names every bad file, in manifest order.
- An artifact directory that is a symlink is followed, not refused — but the
  bucket's names still cannot wander out of wherever it points.

### Removed

- **`litmo.mk_path()`.** An undocumented top-level callable returning the
  packaged `common.mk` by path, unused since it was written: nothing in the
  package called it — `litmo mk` reads the file itself — and no consumer
  imported it. The supported way to get that file is still `litmo mk`, which
  writes it to stdout, and that is how every repository vendors it.

## 0.1.0

First release: `archive`, `mirror` and `fetch` artifact kinds, the shared
`common.mk`, and transparent reading of the two older manifest shapes.
