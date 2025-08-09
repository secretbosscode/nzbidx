FROM python:3.11-slim

WORKDIR /app

# Install dependencies
# ``httpx`` is required for Starlette's TestClient used in the unit tests and
# ``fakeredis`` is needed to exercise caching behaviour without a real Redis
# server.  ``redis`` itself is also installed as it is imported by the
# application code.  These packages were missing from the image and caused the
# tests to fail during collection.
RUN pip install --no-cache-dir \
    fastapi \
    uvicorn[standard] \
    orjson \
    opensearch-py \
    sqlalchemy>=2 \
    asyncpg \
    redis \
    httpx \
    fakeredis

# Copy application source
COPY src/ .

CMD ["uvicorn", "nzbidx_api.main:app", "--host", "0.0.0.0", "--port", "8080"]
