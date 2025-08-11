import os
import sys
from typing import List, Dict

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))

from nzbidx_ingest.parsers import parse
from nzbidx_ingest.main import connect_db, CATEGORY_MAP


class DummyOS:
    def __init__(self) -> None:
        self.calls: List[Dict[str, object]] = []

    def index(
        self,
        index: str,
        id: str,
        body: Dict[str, object],
        refresh: bool = False,
    ) -> None:
        self.calls.append({"index": index, "id": id, "body": body, "refresh": refresh})


def test_parse_indexes_headers() -> None:
    headers = [
        {
            "subject": "Artist-Album-2021-FLAC [music]",
            "date": "Sat, 01 Jun 2024 10:00:00 +0000",
        }
    ]
    db = connect_db()
    dummy = DummyOS()

    parse(headers, db=db, os_client=dummy)

    cur = db.cursor()
    cur.execute("SELECT norm_title, category, language, tags FROM release")
    row = cur.fetchone()
    assert row is not None
    assert row[0] == "artist-album-2021-flac:2024-06-01"
    assert row[1] == CATEGORY_MAP["music"]
    assert "flac" in (row[3] or "")

    assert dummy.calls
    body = dummy.calls[0]["body"]
    assert body["norm_title"] == "artist-album-2021-flac:2024-06-01"
    assert body["category"] == CATEGORY_MAP["music"]
    assert "flac" in body.get("tags", [])
