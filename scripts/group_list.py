#!/usr/bin/env python3
"""Validate, deduplicate and format NNTP group lists."""

from __future__ import annotations

import argparse
import os
import re
from pathlib import Path
from typing import Iterable, List

GROUP_RE = re.compile(r"^[A-Za-z0-9]+(?:[._-][A-Za-z0-9]+)*$")


def _load_groups(lines: Iterable[str]) -> List[str]:
    seen: dict[str, None] = {}
    for raw in lines:
        group = raw.strip()
        if not group:
            continue
        if not GROUP_RE.fullmatch(group):
            raise SystemExit(f"invalid group name: {group}")
        # Use dict to preserve order while deduplicating
        seen.setdefault(group, None)
    return list(seen.keys())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "file",
        nargs="?",
        type=argparse.FileType("r"),
        default="-",
        help="File containing newline-delimited groups (default: stdin)",
    )
    parser.add_argument(
        "--csv",
        action="store_true",
        help="Output groups as a comma-separated string",
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Update NNTP_GROUP_FILE with validated groups",
    )
    args = parser.parse_args()

    groups = _load_groups(args.file)

    output = ",".join(groups) if args.csv else "\n".join(groups)
    print(output)

    if args.update:
        dest = os.environ.get("NNTP_GROUP_FILE")
        if not dest:
            raise SystemExit("NNTP_GROUP_FILE environment variable is not set")
        Path(dest).write_text("\n".join(groups) + "\n")


if __name__ == "__main__":  # pragma: no cover - CLI script
    main()
