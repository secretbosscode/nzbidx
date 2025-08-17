#!/usr/bin/env python3
"""Identify and optionally prune OpenSearch releases missing from the database."""

from __future__ import annotations

import argparse
import itertools
import logging
from typing import Any, Iterable

from opensearchpy.helpers import bulk, scan

from nzbidx_common.os import OS_RELEASES_ALIAS
from nzbidx_ingest.main import connect_db, connect_opensearch


BATCH_SIZE = 1000


def _batched(iterable: Iterable[Any], size: int) -> Iterable[list[Any]]:
    """Yield ``iterable`` in chunks of ``size``."""
    it = iter(iterable)
    while True:
        batch = list(itertools.islice(it, size))
        if not batch:
            break
        yield batch


def _fetch_os_ids(client: Any, batch_size: int) -> Iterable[list[str]]:
    """Yield batches of document IDs from OpenSearch."""
    documents = scan(
        client,
        index=OS_RELEASES_ALIAS,
        query={"query": {"match_all": {}}},
        size=batch_size,
        _source=False,
    )
    for batch in _batched(documents, batch_size):
        yield [doc["_id"] for doc in batch]


def main(argv: list[str] | None = None) -> None:  # pragma: no cover - script
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--batch-size", type=int, default=BATCH_SIZE, help="IDs per query batch"
    )
    parser.add_argument(
        "--prune", action="store_true", help="delete orphaned OpenSearch docs"
    )
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )

    os_client = connect_opensearch()
    if os_client is None:
        raise SystemExit("OpenSearch unavailable")
    conn = connect_db()
    total = 0
    to_delete: list[str] = []

    for ids in _fetch_os_ids(os_client, args.batch_size):
        placeholders = ",".join(["%s"] * len(ids))
        cur = conn.cursor()
        cur.execute(f"SELECT id FROM release WHERE id IN ({placeholders})", ids)
        existing = {str(row[0]) for row in cur.fetchall()}
        orphans = [i for i in ids if i not in existing]
        if orphans:
            logging.info("found %d orphans", len(orphans))
            total += len(orphans)
            to_delete.extend(orphans)

    logging.info("total orphans: %d", total)
    if args.prune and to_delete:
        actions = [
            {"_op_type": "delete", "_index": OS_RELEASES_ALIAS, "_id": i}
            for i in to_delete
        ]
        bulk(os_client, actions)
        logging.info("deleted %d orphaned docs", len(to_delete))


if __name__ == "__main__":
    main()
