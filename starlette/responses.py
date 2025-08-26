import json

try:  # pragma: no cover - optional dependency
    import orjson  # type: ignore
except Exception:  # pragma: no cover - minimal fallback

    class _OrjsonFallback:  # pragma: no cover - trivial
        @staticmethod
        def dumps(obj, *args, **kwargs):
            return json.dumps(obj, **kwargs).encode("utf-8")

    orjson = _OrjsonFallback()  # type: ignore


class Response:  # pragma: no cover - trivial
    def __init__(
        self,
        content: bytes | str,
        *,
        status_code: int = 200,
        media_type: str = "text/plain",
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self.body = content if isinstance(content, bytes) else content.encode("utf-8")
        self.headers = {"content-type": media_type}
        if headers:
            self.headers.update(headers)


class ORJSONResponse(Response):  # pragma: no cover - trivial
    def __init__(self, content: dict, *, status_code: int = 200) -> None:
        super().__init__(
            orjson.dumps(content),
            status_code=status_code,
            media_type="application/json",
        )


class JSONResponse(ORJSONResponse):  # pragma: no cover - backwards compat
    pass
