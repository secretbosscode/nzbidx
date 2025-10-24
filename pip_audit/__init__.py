from __future__ import annotations

from pathlib import Path

from ._bootstrap import load_upstream_module

_upstream = load_upstream_module("pip_audit", alias="_pip_audit_upstream")

__all__ = getattr(_upstream, "__all__", [])
__doc__ = getattr(_upstream, "__doc__", None)
__version__ = getattr(_upstream, "__version__", None)

for name in __all__:
    globals()[name] = getattr(_upstream, name)

for name in ("__author__", "__email__"):
    if hasattr(_upstream, name):
        globals()[name] = getattr(_upstream, name)

# Ensure submodules can be resolved from both the local overrides and the upstream package.
_local_path = Path(__file__).resolve().parent
__path__ = [str(_local_path)]
for entry in getattr(_upstream, "__path__", []):
    if entry not in __path__:
        __path__.append(entry)
