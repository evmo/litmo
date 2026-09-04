"""Bucket credentials, and the S3 client that uses them.

Credentials live in `.r2` at the repository root — git-ignored, one KEY=value
per line — or in the environment, which wins. That ordering is what lets CI
supply them without a file and a laptop supply them without an export.

Reading a *public* mirror needs none of this; see remote.py.

The file is treated the way ssh treats a private key: if it holds a secret
and other users on the machine can read it, loading it is refused rather than
warned about. What is in it is a live write credential for the bucket, and a
bucket readers trust is a bucket that can hand them a file — so the blast
radius of losing it is every reader, not just the maintainer.
"""

from __future__ import annotations

import os
from pathlib import Path

CREDS_NAME = ".r2"
SECRET = "R2_SECRET_ACCESS_KEY"
KEYS = ("R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", SECRET, "R2_BUCKET_NAME")

EXAMPLE = """\
# Cloudflare R2 credentials for this repository's bucket. Copy to `.r2`, which
# is git-ignored, and fill in. Any of these may instead be set in the
# environment, which takes precedence — that is the path for CI.
#
# `R2_BUCKET` is also accepted as an environment-only legacy alias for
# `R2_BUCKET_NAME`.
#
# The token needs Object Read & Write on this one bucket and nothing else.
# Keep the file private — `chmod 600 .r2`. litmo refuses to load a secret
# that other users on the machine can read.
R2_ACCOUNT_ID=
R2_ACCESS_KEY_ID=
R2_SECRET_ACCESS_KEY=
R2_BUCKET_NAME=
"""


def _guard_mode(path: Path) -> None:
    """Refuse a populated credentials file the rest of the machine can read.

    Checked on every load rather than once at setup, because a mode is easy
    to lose again: `cp .r2.example .r2` starts at the umask, an editor that
    writes through a temp file can reset it, and a restore from a backup or
    an archive carries whatever was recorded there.

    Only the *secret* makes the file worth protecting. An account id or a
    bucket name is not a credential, so a template with the secret still
    blank is left alone — otherwise the check would fire on `.r2` files that
    exist only to name the bucket while CI supplies the key.
    """
    try:
        mode = path.stat().st_mode & 0o777
    except OSError:                          # raced with a delete; load fails
        return                               # on the missing-keys path below
    if mode & 0o077:
        raise SystemExit(
            f"{path} holds {SECRET} and is readable by other users on this "
            f"machine (mode {mode:04o}).\n"
            f"  chmod 600 {path}\n"
            f"  and rotate the token if the machine is shared"
        )


def load(root: Path, bucket_default: str = "") -> dict[str, str]:
    creds: dict[str, str] = {}
    path = root / CREDS_NAME
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            creds[k.strip()] = v.strip().strip('"').strip("'")
        # After the parse, so it fires on what the file actually contains,
        # and before the environment overlay, so an env-supplied key cannot
        # mask a secret still sitting on disk in a readable file.
        if creds.get(SECRET):
            _guard_mode(path)
    for k in KEYS:
        if os.environ.get(k):
            creds[k] = os.environ[k]
    # `R2_BUCKET` is what one of these repos used before; accept both.
    if not creds.get("R2_BUCKET_NAME"):
        creds["R2_BUCKET_NAME"] = os.environ.get("R2_BUCKET", "") or bucket_default

    missing = [k for k in KEYS if not creds.get(k)]
    if missing:
        raise SystemExit(
            f"missing R2 credentials: {', '.join(missing)}\n"
            f"  put them in {path} (see .r2.example) or the environment"
        )
    return creds


def client(creds: dict[str, str]):
    try:
        import boto3
        from botocore.config import Config
    except ImportError:
        raise SystemExit(
            "this needs the S3 client — install litmo with the `s3` extra:\n"
            "  uv add 'litmo[s3] @ git+https://github.com/evmo/litmo'"
        ) from None

    return boto3.client(
        "s3",
        endpoint_url=f"https://{creds['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        aws_access_key_id=creds["R2_ACCESS_KEY_ID"],
        aws_secret_access_key=creds["R2_SECRET_ACCESS_KEY"],
        region_name="auto",
        # Two R2 quirks, both learned the hard way:
        #  - it rejects botocore's default aws-chunked checksum trailers;
        #  - it closes long multipart connections often enough that five
        #    attempts are not enough for a 250 MB upload.
        config=Config(retries={"max_attempts": 10, "mode": "adaptive"},
                      connect_timeout=30, read_timeout=300,
                      request_checksum_calculation="when_required",
                      response_checksum_validation="when_required"),
    )


def transfer_config():
    from boto3.s3.transfer import TransferConfig
    return TransferConfig(multipart_threshold=64 * 1024 * 1024,
                          multipart_chunksize=64 * 1024 * 1024,
                          max_concurrency=4, use_threads=True)
