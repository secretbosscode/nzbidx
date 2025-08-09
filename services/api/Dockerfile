FROM python:3.11-slim

WORKDIR /app

# Install dependencies
# Include httpx to support Starlette's TestClient used in tests
RUN pip install --no-cache-dir fastapi uvicorn[standard] orjson opensearch-py sqlalchemy>=2 asyncpg httpx

# Copy application source
COPY src/ .

CMD ["uvicorn", "nzbidx_api.main:app", "--host", "0.0.0.0", "--port", "8080"]
