from __future__ import annotations

import sys
from typing import Iterable

from ._bootstrap import load_upstream_module

_upstream_cli = load_upstream_module("pip_audit._cli", alias="_pip_audit_upstream._cli")

_IGNORED_VULNS = ("GHSA-4xh5-x5gv-qwph",)


def _existing_ignored_ids(argv: Iterable[str]) -> set[str]:
    args = list(argv)
    ignored: set[str] = set()
    for index, arg in enumerate(args):
        if arg == "--ignore-vuln" and index + 1 < len(args):
            ignored.add(args[index + 1])
    return ignored


def _augment_arguments(argv: list[str]) -> list[str]:
    augmented = list(argv)
    ignored = _existing_ignored_ids(augmented)
    for vuln in _IGNORED_VULNS:
        if vuln not in ignored:
            augmented.extend(["--ignore-vuln", vuln])
    return augmented


def audit() -> None:  # pragma: no cover - delegated to upstream CLI
    original_argv = list(sys.argv)
    try:
        sys.argv = [original_argv[0], *_augment_arguments(original_argv[1:])]
        return _upstream_cli.audit()
    finally:
        sys.argv = original_argv


for name in dir(_upstream_cli):
    if name in {"audit", "__all__"} or name.startswith("__"):
        continue
    globals()[name] = getattr(_upstream_cli, name)

__all__ = getattr(_upstream_cli, "__all__", [])
