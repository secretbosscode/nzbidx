import asyncio
import os
from types import SimpleNamespace

os.environ["API_KEYS"] = "secret"

import nzbidx_api.main as main  # type: ignore


def test_api_returns_extended_attrs(monkeypatch):
    monkeypatch.setattr(
        main,
        "_search",
        lambda *args, **kwargs: [
            {
                "title": "t",
                "guid": "1",
                "pubDate": "Mon, 01 Jan 2024 00:00:00 +0000",
                "category": "2030",
                "link": "/api?t=getnzb&id=1",
                "size": "123",
                "imdbid": "tt1234567",
            }
        ],
    )
    request = SimpleNamespace(
        query_params={"t": "search", "extended": "1", "apikey": "secret"},
        headers={},
        url=None,
    )
    resp = asyncio.run(main.api(request))
    xml = resp.body.decode()
    assert '<attr name="imdbid" value="tt1234567"/>' in xml
    assert '<attr name="size" value="123"/>' in xml
    assert '<attr name="category" value="2030"/>' in xml

