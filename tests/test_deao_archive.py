from datetime import date

from src.tasks.deao.archive import (
    archive_key,
    list_archived_days,
    pack,
    read_day_archive,
    unpack,
    write_day_archive,
)
from tests.deao_fakes import fake_s3  # noqa: F401


def test_pack_unpack_roundtrip():
    files = {"26-2.xml": b"<b/>", "26-1.xml": b"<a/>"}
    assert unpack(pack(files)) == files
    assert pack(files) == pack(dict(reversed(list(files.items()))))


def test_pack_vide():
    assert unpack(pack({})) == {}


def test_write_read_list(fake_s3):  # noqa: F811
    write_day_archive("boamp/raw", date(2026, 10, 14), {"26-1.xml": b"<a/>"})
    assert (
        archive_key("boamp/raw", date(2026, 10, 14)) == "boamp/raw/2026/10/14.tar.zst"
    )
    assert read_day_archive("boamp/raw", date(2026, 10, 14)) == {"26-1.xml": b"<a/>"}
    fake_s3.put_object(Bucket="b", Key="boamp/raw/readme.txt", Body=b"")
    assert list_archived_days("boamp/raw") == {date(2026, 10, 14)}
