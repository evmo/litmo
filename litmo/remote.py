"""Transport — reads over public HTTPS when possible, S3 when not.

Reads and writes are deliberately asymmetric. If `remote.base` is set in
sync.toml the bucket is served publicly, so `pull` and `status` are plain
HTTPS and a reader needs no account anywhere. Writes always go over the S3
API with credentials, because publishing is a maintainer's job.

Two things a reader is not allowed to assume: that an object key is safe to
paste into a URL (it is percent-encoded, so a key with a space or a `#` in it
addresses the object rather than something adjacent to it), and that a body
is the size it was promised to be (a download stops at the size the manifest
declared rather than filling the disk).
"""

from __future__ import annotations

import concurrent.futures as cf
import contextlib
import http.client
import os
import random
import socket
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from . import creds as _creds
from .hashing import human

UA = "litmo/0.1 (+https://github.com/evmo/litmo)"
TIMEOUT = 120
CHUNK = 1 << 20

# Reads get a few attempts; writes get none here, because botocore already
# retries those ten times over — R2 closes long multipart connections, and
# creds.client is configured for it. The read path is the one most people
# exercise, and a single blip on it used to cost a whole multi-gigabyte pull:
# staged files live in a temporary directory and nothing is installed until
# every file verifies, so a re-run starts from zero. Three attempts is enough
# for one dropped connection and short enough that a network which is
# genuinely gone still fails promptly.
ATTEMPTS = 3
BACKOFF = 0.5

# The `fetch` kind has no manifest, so nothing promises how big its file is —
# unlike an archive, there is no honest number to hold the body to and refuse
# for the lack of. A ceiling is the only bound available. It is deliberately
# far above anything real: the largest `fetch` any consumer configures today
# is 645 KB (catracker's all-positions.csv.gz), so this leaves four orders of
# magnitude for a repository that fetches a multi-GB grid.
MAX_FETCH = 8 << 30


def _request(url: str, headers: dict[str, str] | None = None):
    return urllib.request.Request(url, headers={"User-Agent": UA, **(headers or {})})


def _url(base: str, key: str) -> str:
    """`base/key`, with the key's path segments percent-encoded."""
    return f"{base}/{urllib.parse.quote(key, safe='/')}"


def _drain(reader, fh, max_bytes: int | None) -> int:
    """Copy a stream to a file, refusing to exceed `max_bytes`."""
    total = 0
    while chunk := reader.read(CHUNK):
        total += len(chunk)
        if max_bytes is not None and total > max_bytes:
            raise Oversized(f"longer than the {max_bytes:,} bytes promised")
        fh.write(chunk)
    return total


def _why(e: BaseException) -> str:
    return f"{type(e).__name__}: {getattr(e, 'reason', None) or e}"


def _transient(e: BaseException) -> bool:
    """Is this worth another attempt — the connection, or the answer?

    A 404 or a 403 will say the same thing next time, and an oversized body
    is not going to shrink. A reset socket, a truncated read, a name that
    would not resolve or a 5xx might all be gone by the next request.
    """
    if isinstance(e, urllib.error.HTTPError):
        return e.code in (408, 425, 429, 500, 502, 503, 504)
    if isinstance(e, urllib.error.URLError) and isinstance(e.reason, BaseException):
        e = e.reason
    return isinstance(e, (ConnectionError, TimeoutError, socket.gaierror,
                          ssl.SSLError, http.client.IncompleteRead))


def _retrying(attempt, what: str):
    """Run `attempt`, retrying a transient failure with a jittered backoff.

    Every read this wraps is safe to repeat: each attempt writes to a fresh
    `.part` file or an in-memory buffer, and what comes back is checked
    against the manifest's size and digest afterwards. A second attempt can
    only cost time.
    """
    for n in range(1, ATTEMPTS + 1):
        try:
            return attempt()
        except Exception as e:
            if n == ATTEMPTS or not _transient(e):
                raise
            pause = BACKOFF * 2 ** (n - 1) * (0.5 + random.random())
            print(f"    {what}: {_why(e)} — retrying in {pause:.1f}s "
                  f"({n} of {ATTEMPTS - 1})", flush=True)
            time.sleep(pause)


class Missing(Exception):
    """The object is not in the bucket."""


class Oversized(Exception):
    """The body is larger than the manifest said it would be."""


class Conflict(SystemExit):
    """Someone else wrote the manifest while this push was running."""


class Remote:
    def __init__(self, cfg, *, need_write: bool = False):
        self.cfg = cfg
        self.public = bool(cfg.base) and not need_write
        self._s3 = None
        self._bucket = cfg.bucket
        if not self.public:
            c = _creds.load(cfg.root, cfg.bucket)
            self._s3 = _creds.client(c)
            self._bucket = c["R2_BUCKET_NAME"]

    # --- description, for messages -----------------------------------------

    @property
    def where(self) -> str:
        return self.cfg.base if self.public else f"s3://{self._bucket}"

    # --- reads --------------------------------------------------------------

    def get_bytes(self, key: str, *, limit: int | None = None
                  ) -> tuple[bytes, str | None]:
        """(body, etag). The etag is the precondition for writing it back."""
        if self.public:
            def once():
                with urllib.request.urlopen(_request(_url(self.cfg.base, key)),
                                            timeout=TIMEOUT) as r:
                    body = r.read(limit + 1) if limit is not None else r.read()
                    if limit is not None and len(body) > limit:
                        raise Oversized(f"{key} is larger than {limit:,} bytes")
                    return body, (r.headers.get("ETag") or "").strip('"') or None
            try:
                return _retrying(once, key)
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    raise Missing(key) from None
                raise
        import botocore.exceptions
        try:
            obj = self._s3.get_object(Bucket=self._bucket, Key=key)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
                raise Missing(key) from None
            raise
        with obj["Body"] as body_stream:
            body = (body_stream.read(limit + 1) if limit is not None
                    else body_stream.read())
        if limit is not None and len(body) > limit:
            raise Oversized(f"{key} is larger than {limit:,} bytes")
        return body, (obj.get("ETag") or "").strip('"') or None

    def download(self, key: str, dest: Path, max_bytes: int | None = None) -> None:
        """Fetch one object to `dest`, leaving nothing behind on failure.

        `max_bytes` is enforced while the body streams, on both paths. The
        public one counts in `_drain`. The S3 one used to enforce nothing at
        all — boto3 owns the transfer, and the excuse was that the caller
        checks size and digest against the manifest afterwards. It does, but
        only once the whole object is on disk: an object far larger than the
        manifest admits to filled staging before anything looked at it.

        The transfer's own progress callback is the bound. s3transfer invokes
        it as bytes land and rewinds a retried part with a *negative* delta,
        so the running total is what has arrived rather than what has been
        attempted — an object of exactly the promised size never trips, and a
        dropped connection re-reading its part does not either. Raising from
        it aborts the transfer part-way instead of after the fact.
        """
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(dest.suffix + ".part")
        def once():
            with urllib.request.urlopen(_request(_url(self.cfg.base, key)),
                                        timeout=TIMEOUT) as r, \
                    tmp.open("wb") as fh:
                _drain(r, fh, max_bytes)

        seen, counted = 0, threading.Lock()

        def landed(n: int) -> None:
            """Called from s3transfer's worker threads, one part each."""
            nonlocal seen
            with counted:
                seen += n
                over = seen > max_bytes
            if over:
                raise Oversized(f"longer than the {max_bytes:,} bytes promised")

        try:
            if self.public:
                # Each attempt reopens `tmp` for writing, so a retry starts
                # from an empty file rather than appending to half of one.
                _retrying(once, key)
            else:
                # The same transfer settings the upload path uses: one
                # bucket, one set of R2 quirks. Fewer parts in flight also
                # tightens the cap above — measured against a 400 MB object
                # promised 1 MB, the abort lands after 12-15 MB rather than
                # the 19-30 MB boto3's 10-way default pulls first — and it
                # keeps `download_many`'s 8 workers from each spawning ten
                # more threads on any file over boto3's 8 MB threshold.
                self._s3.download_file(
                    self._bucket, key, str(tmp),
                    Config=_creds.transfer_config(),
                    **({} if max_bytes is None else {"Callback": landed}))
            os.replace(tmp, dest)
        except Oversized as e:
            with contextlib.suppress(OSError):
                tmp.unlink()
            raise Oversized(f"{key} is {e}") from None
        except BaseException:
            with contextlib.suppress(OSError):
                tmp.unlink()
            raise

    def download_many(self, jobs: list[tuple], workers: int = 8,
                      label: str = "") -> None:
        """(key, dest[, max_bytes]) triples, concurrently. Raises on the first
        failure, and drops whatever has not started.

        `with ThreadPoolExecutor` would shut down without `cancel_futures`, so
        every queued job still ran to completion before the exception the
        caller is waiting for surfaced. On a network that has gone away each
        of those spends the full socket timeout, which turns a thousand-file
        pull into hours of waiting for an error that was already decided. The
        in-flight ones still have to finish; the rest are cancelled.
        """
        if not jobs:
            return
        done = 0
        pool = cf.ThreadPoolExecutor(max_workers=max(1, workers))
        try:
            futures = {pool.submit(self.download, *j): j for j in jobs}
            for fut in cf.as_completed(futures):
                key, dest = futures[fut][0], futures[fut][1]
                fut.result()
                done += 1
                print(f"    [{done}/{len(jobs)}] {human(dest.stat().st_size):>10}"
                      f"  {key}", flush=True)
        finally:
            pool.shutdown(wait=True, cancel_futures=True)

    # --- writes -------------------------------------------------------------

    def upload(self, src: Path, key: str, content_type: str) -> None:
        self._s3.upload_file(str(src), self._bucket, key,
                             ExtraArgs={"ContentType": content_type},
                             Config=_creds.transfer_config())

    def put_bytes(self, key: str, body: bytes, content_type: str, *,
                  if_match: str | None = None,
                  if_absent: bool = False) -> None:
        """Write an object, optionally only if it is still what we read.

        `if_match` is the ETag this run loaded; `if_absent` says the object
        was not there at all. Either way a second maintainer who published
        between the read and this write gets a refusal rather than having
        their entries dropped. A store that does not implement conditional
        writes is refused: publishing without the precondition would silently
        discard that safety property.
        """
        import botocore.exceptions
        extra = {}
        if if_match:
            extra["IfMatch"] = f'"{if_match.strip(chr(34))}"'
        elif if_absent:
            extra["IfNoneMatch"] = "*"
        try:
            self._s3.put_object(Bucket=self._bucket, Key=key, Body=body,
                                ContentType=content_type, **extra)
        except botocore.exceptions.ClientError as e:
            if not extra:
                raise
            code = e.response["Error"]["Code"]
            status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if code in ("PreconditionFailed", "ConditionalRequestConflict") \
                    or status in (412, 409):
                # botocore retries a put whose connection died on the way
                # back — creds.client asks for ten attempts, because R2 drops
                # long ones — and the retry carries the same precondition, so
                # it can be refused by the first attempt's own write. Read the
                # object back before telling a maintainer someone else
                # published: a two-person workflow acts on that sentence.
                with contextlib.suppress(Exception):
                    if self.get_bytes(key)[0] == body:
                        print(f"  {key} is already what this push wrote — "
                              f"the answer to the write was lost, not the "
                              f"write")
                        return
                raise Conflict(
                    f"  {key} changed in {self.where} while this push was "
                    f"running — someone else published.\n"
                    f"  Nothing further was written. Re-run `litmo push`; it "
                    f"will re-read the manifest and merge."
                ) from None
            if code in ("NotImplemented", "InvalidRequest") or status == 501:
                raise SystemExit(
                    f"  {self.where} does not support conditional writes, so "
                    f"{key} was not written.\n"
                    f"  Publishing it unconditionally could overwrite "
                    f"another maintainer's push; use an object store that "
                    f"supports If-Match and If-None-Match."
                ) from None
            raise


def head(url: str) -> dict[str, str]:
    """ETag / Last-Modified / Content-Length for an arbitrary URL.

    Used by the `fetch` kind, whose files are published by something outside
    this repository and so have no manifest to compare against.
    """
    req = _request(url)
    req.get_method = lambda: "HEAD"
    with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
        h = r.headers
        return {k: v for k, v in (
            ("etag", (h.get("ETag") or "").strip('"')),
            ("last_modified", h.get("Last-Modified") or ""),
            ("size", h.get("Content-Length") or ""),
        ) if v}


def fetch_url(url: str, dest: Path) -> dict[str, str]:
    """Download an arbitrary URL, returning its validators.

    Held to `MAX_FETCH`. This was the one read path in litmo with no bound
    at all — `download` takes the size the manifest promised, `get_bytes` a
    limit, the manifest itself `MAX_BYTES` — excused on the grounds that a
    `fetch` url is trusted config. It is the *host* that is chosen, though,
    not what it serves: `urlopen` follows redirects, so the body need not
    come from the configured URL. Measured against a local server: a HEAD
    advertising 12 bytes, a 302 off the configured path, and 8,388,608 bytes
    written without complaint. That is also why the HEAD's Content-Length is
    not used as the bound — it describes a response the GET need not receive.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")

    def once():
        with urllib.request.urlopen(_request(url), timeout=TIMEOUT) as r, \
                tmp.open("wb") as fh:
            _drain(r, fh, MAX_FETCH)
            h = r.headers
            return {"etag": (h.get("ETag") or "").strip('"'),
                    "last_modified": h.get("Last-Modified") or ""}

    try:
        meta = _retrying(once, url)
        os.replace(tmp, dest)
    except Oversized:
        # `_drain` says "promised", which is the archive's case — there the
        # number came from the manifest. Nothing promised this one.
        with contextlib.suppress(OSError):
            tmp.unlink()
        raise Oversized(f"larger than the {MAX_FETCH:,} bytes a fetch is "
                        f"held to") from None
    except BaseException:
        with contextlib.suppress(OSError):
            tmp.unlink()
        raise
    return {k: v for k, v in meta.items() if v}
