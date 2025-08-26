from nzbidx_api.log_sanitize import scrub_headers


def test_scrub_headers_redacts_sensitive() -> None:
    headers = {"Authorization": "secret", "X-Test": "value"}
    result = scrub_headers(headers)
    assert result is not headers
    assert result["Authorization"] == "[redacted]"
    assert result["X-Test"] == "value"
    # original mapping remains unmodified
    assert headers["Authorization"] == "secret"


def test_scrub_headers_passthrough_when_no_sensitive() -> None:
    headers = {"X-Test": "value"}
    result = scrub_headers(headers)
    assert result is headers
    assert result["X-Test"] == "value"
