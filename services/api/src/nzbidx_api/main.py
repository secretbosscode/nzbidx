"""API service entrypoint using FastAPI."""

import json
import logging
import os
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.responses import ORJSONResponse
from opensearchpy import OpenSearch

from .db import ping
from .newznab import caps_xml

logger = logging.getLogger(__name__)

app = FastAPI(default_response_class=ORJSONResponse)


@app.on_event("startup")
def init_opensearch() -> None:
    """Connect to OpenSearch and ensure indices exist."""
    url = os.getenv("OPENSEARCH_URL", "http://opensearch:9200")
    try:
        client = OpenSearch(url, timeout=2)
        template_path = Path(__file__).resolve().parents[3] / "opensearch" / "index-template.json"
        with template_path.open("r", encoding="utf-8") as f:
            template_body = json.load(f)
        client.indices.put_index_template(name="nzbidx-releases-template", body=template_body)
        if not client.indices.exists(index="nzbidx-releases-v1"):
            client.indices.create(index="nzbidx-releases-v1")
        logger.info("OpenSearch ready")
    except Exception as exc:  # pragma: no cover - optional dependency
        logger.warning("OpenSearch unavailable: %s", exc)


@app.get("/health")
async def health() -> dict[str, str]:
    """Health check endpoint."""
    db_status = "ok" if await ping() else "down"
    return {"status": "ok", "db": db_status}


@app.get("/api")
async def api(t: str = Query(...)) -> Response:
    """Newznab compatible endpoint.

    Currently supports only the ``caps`` request type.
    """
    if t == "caps":
        return Response(content=caps_xml(), media_type="application/xml")
    raise HTTPException(status_code=400, detail="unsupported request")


if __name__ == "__main__":  # pragma: no cover - convenience for manual runs
    import uvicorn

    uvicorn.run("nzbidx_api.main:app", host="0.0.0.0", port=8080)
