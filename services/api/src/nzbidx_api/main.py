"""API service entrypoint using FastAPI."""

from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.responses import ORJSONResponse

from .newznab import caps_xml

app = FastAPI(default_response_class=ORJSONResponse)


@app.get("/health")
async def health() -> dict[str, str]:
    """Health check endpoint."""
    return {"status": "ok"}


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
