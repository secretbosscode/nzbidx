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
    __slots__ = ("status_code", "body", "headers")

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


class ORJSONResponse(Response):  # pragma: no cover - trivial
    __slots__ = ()

    def __init__(self, content: dict, *, status_code: int = 200) -> None:
        self.status_code = status_code
        self.body = orjson.dumps(content)
        self.headers = {"content-type": "application/json"}


class JSONResponse(ORJSONResponse):  # pragma: no cover - backwards compat
    pass
