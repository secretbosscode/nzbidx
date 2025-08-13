import json


class Response:  # pragma: no cover - trivial
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


class JSONResponse(Response):  # pragma: no cover - trivial
    def __init__(self, content: dict, *, status_code: int = 200) -> None:
        super().__init__(
            json.dumps(content), status_code=status_code, media_type="application/json"
        )
