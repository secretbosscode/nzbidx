"""Utility script to seed OpenSearch with sample release data."""

from __future__ import annotations

import os

from dotenv import load_dotenv
from opensearchpy import OpenSearch
from nzbidx_common.os import OS_RELEASES_ALIAS


def main() -> None:
    """Seed OpenSearch with sample releases."""
    load_dotenv()
    url = os.environ["OPENSEARCH_URL"]
    client = OpenSearch(url)

    releases = [
        {
            "norm_title": "Fake Release 1",
            "category": "movies",
            "posted_at": "2024-01-01T00:00:00Z",
            "size_bytes": 123,
            "language": "en",
            "tags": ["fake", "release", "1"],
        },
        {
            "norm_title": "Fake Release 2",
            "category": "tv",
            "posted_at": "2024-01-02T00:00:00Z",
            "size_bytes": 456,
            "language": "en",
            "tags": ["fake", "release", "2"],
        },
        {
            "norm_title": "Fake Release 3",
            "category": "music",
            "posted_at": "2024-01-03T00:00:00Z",
            "size_bytes": 789,
            "language": "en",
            "tags": ["fake", "release", "3"],
        },
    ]

    for idx, body in enumerate(releases, start=1):
        client.index(
            index=OS_RELEASES_ALIAS, id=str(idx), body=body, refresh="wait_for"
        )

    print("Seeded 3 releases")


if __name__ == "__main__":
    main()
