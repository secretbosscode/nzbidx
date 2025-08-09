from __future__ import annotations

import os

from dotenv import load_dotenv
from opensearchpy import OpenSearch


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
        },
        {
            "norm_title": "Fake Release 2",
            "category": "tv",
            "posted_at": "2024-01-02T00:00:00Z",
            "size_bytes": 456,
        },
        {
            "norm_title": "Fake Release 3",
            "category": "music",
            "posted_at": "2024-01-03T00:00:00Z",
            "size_bytes": 789,
        },
    ]

    for idx, body in enumerate(releases, start=1):
        client.index(index="nzbidx-releases-v1", id=str(idx), body=body, refresh=True)

    print("Seeded 3 releases")


if __name__ == "__main__":
    main()
