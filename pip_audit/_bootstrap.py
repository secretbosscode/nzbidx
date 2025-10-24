from __future__ import annotations

import importlib.util
import sys
from importlib.machinery import PathFinder
from pathlib import Path
from types import ModuleType

_REPO_ROOT = Path(__file__).resolve().parents[1]


def _is_repo_path(entry: str) -> bool:
    try:
        return Path(entry).resolve() == _REPO_ROOT
    except (OSError, RuntimeError):
        return False


def upstream_search_paths() -> list[str]:
    """Return sys.path entries that do not point at the repository root."""
    return [entry for entry in sys.path if not _is_repo_path(entry)]


def load_upstream_module(name: str, alias: str | None = None) -> ModuleType:
    """Load a module from the real ``pip_audit`` package installed in site-packages."""

    module_alias = alias or f"_pip_audit_upstream.{name}"
    existing = sys.modules.get(module_alias)
    if existing is not None:
        return existing  # type: ignore[return-value]

    if "." in name:
        parent_name, _, _ = name.rpartition(".")
        parent_alias = f"_pip_audit_upstream.{parent_name}"
        parent_module = load_upstream_module(parent_name, alias=parent_alias)
        search_paths = getattr(parent_module, "__path__", None)
    else:
        search_paths = upstream_search_paths()

    spec = PathFinder.find_spec(name, search_paths)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to locate upstream module {name!r}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_alias] = module
    loader = spec.loader
    loader.exec_module(module)
    return module
