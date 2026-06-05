import io
import tarfile
from datetime import date

import src.tasks.boamp.fetch as fetch
from src.tasks.boamp.fetch import (
    day_index_url,
    iter_archive_xml,
    iter_days,
    list_day_files,
    year_archive_urls,
)

INDEX_HTML = """
<html><body><pre>
<a href="?C=N;O=D">Name</a>
<a href="/parent/">Parent Directory</a>
<a href="26-10240.xml">26-10240.xml</a> 2026-01-31 14:15 86K
<a href="26-10979.xml">26-10979.xml</a> 2026-01-31 23:45 32K
<a href="26-10240.xml">26-10240.xml</a> dup
</pre></body></html>
"""


def test_day_index_url_routing():
    assert day_index_url(date(2025, 6, 15)).endswith(
        "/FluxHistorique/Boamp_v240/2025/06/15/"
    )
    assert day_index_url(date(2026, 1, 31)).endswith("/2026/01/31/")


def test_iter_days_inclusive():
    days = list(iter_days(date(2026, 1, 30), date(2026, 2, 1)))
    assert days == [date(2026, 1, 30), date(2026, 1, 31), date(2026, 2, 1)]


def test_list_day_files_parses_and_dedupes(monkeypatch):
    monkeypatch.setattr(
        fetch, "stream_get", lambda url, **kw: iter([INDEX_HTML.encode("utf-8")])
    )
    files = list_day_files(date(2026, 1, 31))
    assert files == ["26-10240.xml", "26-10979.xml"]


def test_year_archive_urls_uses_ao_only(monkeypatch):
    urls = year_archive_urls(2024)
    assert all(u.endswith(".tar.gz") for u in urls)
    assert any("MAPA-AO" in u for u in urls)
    assert not any("IC-AA" in u for u in urls)  # attributions exclues


def _make_targz(members: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        for name, content in members.items():
            info = tarfile.TarInfo(name=name)
            info.size = len(content)
            tf.addfile(info, io.BytesIO(content))
    return buf.getvalue()


def test_iter_archive_xml_yields_xml_members(monkeypatch):
    targz = _make_targz(
        {"a/26-1.xml": b"<ann/>", "a/readme.txt": b"x", "a/26-2.xml": b"<ann/>"}
    )
    monkeypatch.setattr("src.tasks.boamp.fetch.year_archive_urls", lambda y: ["u1"])
    monkeypatch.setattr(
        "src.tasks.boamp.fetch.stream_get", lambda url, **kw: iter([targz])
    )
    out = list(iter_archive_xml(2024))
    names = sorted(n for n, _ in out)
    assert names == ["a/26-1.xml", "a/26-2.xml"]
    assert all(content == b"<ann/>" for _, content in out)
