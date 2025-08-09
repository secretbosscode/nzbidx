FROM python:3.11-slim

WORKDIR /app

# Install dependencies (API + tests)
# - httpx: used by Starlette/FastAPI TestClient
# - redis: required by app code
# - fakeredis: enables cache tests without a real Redis server
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
