"""Archive brute des XML DILA sur S3 : un tar compressé zstd par jour."""

import io
import re
import tarfile
from datetime import date

import zstandard

from src.config import DEAO_S3_PREFIX, S3_BUCKET
from src.tasks.utils import s3_client


def pack(files: dict[str, bytes]) -> bytes:
    """Tar en mémoire compressé zstd, déterministe (entrées triées, métadonnées fixes)."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        for name in sorted(files):
            info = tarfile.TarInfo(name)
            info.size = len(files[name])
            info.mtime = 0
            info.uid = info.gid = 0
            info.uname = info.gname = ""
            info.mode = 0o644
            tar.addfile(info, io.BytesIO(files[name]))
    return zstandard.ZstdCompressor().compress(buf.getvalue())


def unpack(data: bytes) -> dict[str, bytes]:
    raw = zstandard.ZstdDecompressor().decompressobj().decompress(data)
    files = {}
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r") as tar:
        for member in tar:
            if member.isfile():
                files[member.name] = tar.extractfile(member).read()
    return files


def archive_key(d: date) -> str:
    return f"{DEAO_S3_PREFIX}/{d:%Y}/{d:%m}/{d:%d}.tar.zst"


def list_archived_days() -> set[date]:
    pattern = re.compile(
        rf"^{re.escape(DEAO_S3_PREFIX)}/(\d{{4}})/(\d{{2}})/(\d{{2}})\.tar\.zst$"
    )
    days = set()
    paginator = s3_client().get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=f"{DEAO_S3_PREFIX}/"):
        for obj in page.get("Contents", []):
            m = pattern.match(obj["Key"])
            if m:
                try:
                    days.add(date(*map(int, m.groups())))
                except ValueError:
                    continue
    return days


def read_day_archive(d: date) -> dict[str, bytes]:
    obj = s3_client().get_object(Bucket=S3_BUCKET, Key=archive_key(d))
    return unpack(obj["Body"].read())


def write_day_archive(d: date, files: dict[str, bytes]) -> None:
    s3_client().put_object(Bucket=S3_BUCKET, Key=archive_key(d), Body=pack(files))
