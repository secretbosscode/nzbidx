"""Thin NZB builder interface.

This module exposes :func:`build_nzb_for_release` which currently returns a
stub NZB document.  A real implementation can later plug in a body fetcher
without changing the public API.
"""

from __future__ import annotations


def build_nzb_for_release(release_id: str) -> str:
    """Return an NZB XML document for ``release_id``.

    The default implementation simply delegates to
    :func:`nzbidx_api.newznab.nzb_xml_stub`.
    """
    from .newznab import nzb_xml_stub

    return nzb_xml_stub(release_id)
