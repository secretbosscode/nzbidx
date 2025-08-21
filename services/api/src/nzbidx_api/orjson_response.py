from __future__ import annotations

from .json_utils import orjson

try:  # pragma: no cover - optional dependency
    from starlette.responses import Response as StarletteResponse
except Exception:  # pragma: no cover - minimal fallback

    class StarletteResponse:  # type: ignore
        """Very small subset of Starlette's Response used for tests."""

        def __init__(
            self,
            content: str,
            *,
            status_code: int = 200,
            media_type: str = "text/plain",
            headers: dict[str, str] | None = None,
        ) -> None:
            self.status_code = status_code
            self.body = content.encode("utf-8")
            self.headers = {"content-type": media_type}
            if headers:
                self.headers.update(headers)


class ORJSONResponse(StarletteResponse):  # pragma: no cover - simple wrapper
    """Response class that renders content using orjson."""

    media_type = "application/json"

    def __init__(self, content: dict, *, status_code: int = 200) -> None:
        super().__init__(
            orjson.dumps(content).decode(),
            status_code=status_code,
            media_type=self.media_type,
        )


# Re-export Response for modules that require it
Response = StarletteResponse
