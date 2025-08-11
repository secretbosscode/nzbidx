FROM python:3.13-slim

RUN addgroup --system --gid 1000 nzbidx && \
    adduser --system --uid 1000 --gid 1000 nzbidx

WORKDIR /app

RUN pip install --no-cache-dir \
    python-dotenv \
    opensearch-py

COPY services/ingest/src/ ./
COPY nzbidx_common/ ./nzbidx_common

USER nzbidx
VOLUME /tmp

CMD ["python", "-m", "nzbidx_ingest"]
