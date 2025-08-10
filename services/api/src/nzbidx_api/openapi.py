"""Very small OpenAPI schema for documentation."""

from __future__ import annotations

from starlette.responses import JSONResponse

OPENAPI = {
    "openapi": "3.0.0",
    "info": {"title": "nzbidx API", "version": "1.0"},
    "paths": {
        "/api": {
            "get": {
                "parameters": [
                    {"name": "t", "in": "query", "schema": {"type": "string"}},
                    {
                        "name": "X-Request-ID",
                        "in": "header",
                        "schema": {"type": "string"},
                    },
                    {
                        "name": "X-Api-Key",
                        "in": "header",
                        "schema": {"type": "string"},
                    },
                ]
            }
        }
    },
}


def openapi_json(request):
    return JSONResponse(OPENAPI)
