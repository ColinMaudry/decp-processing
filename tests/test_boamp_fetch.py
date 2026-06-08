from datetime import date

import src.tasks.boamp.fetch as fetch
from src.tasks.boamp.fetch import (
    day_index_url,
    iter_days,
    list_day_files,
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
