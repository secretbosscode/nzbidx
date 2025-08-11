FROM python:3.13-slim

RUN addgroup --system --gid 1000 nzbidx && \
    adduser --system --uid 1000 --gid 1000 nzbidx

WORKDIR /app

RUN pip install --no-cache-dir \
    fastapi \
    uvicorn[standard] \
    orjson \
    opensearch-py \
    sqlalchemy>=2 \
    asyncpg \
    redis \
    httpx \
    fakeredis \
    python-dotenv

COPY services/api/src/ ./
COPY nzbidx_common/ ./nzbidx_common

USER nzbidx
VOLUME /tmp

CMD ["uvicorn", "nzbidx_api.main:app", "--host", "0.0.0.0", "--port", "8080"]
