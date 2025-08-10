class Request:  # pragma: no cover - simple container
    def __init__(self, scope: dict) -> None:
        self.query_params = scope.get("query_params", {})
        self.headers = scope.get("headers", {})
        self.url = scope.get("url")
        self.state = scope.get("state", object())
