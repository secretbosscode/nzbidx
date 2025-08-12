from typing import Callable, Optional, List


class Route:  # pragma: no cover - trivial holder
    def __init__(
        self, path: str, endpoint: Callable, methods: Optional[List[str]] = None
    ) -> None:
        self.path = path
        self.endpoint = endpoint
        self.methods = methods or ["GET"]
