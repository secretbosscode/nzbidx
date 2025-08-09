"""Helpers for the Newznab API."""


def caps_xml() -> str:
    """Return a minimal Newznab caps XML document."""
    return (
        "<caps><server version=\"0.1\" title=\"nzbidx\"/>"
        "<limits max=\"100\" default=\"50\"/></caps>"
    )
