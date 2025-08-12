FROM python:3.13-slim

RUN addgroup --system --gid 1000 nzbidx && \
    adduser --system --uid 1000 --gid 1000 nzbidx

WORKDIR /app

COPY services/api/ services/api/
COPY services/ingest/ services/ingest/
# Schema file required by the API when applying migrations
COPY db/init/schema.sql services/api/src/db/init/schema.sql

RUN pip install --no-cache-dir -e services/api -e services/ingest

COPY nzbidx_common/ nzbidx_common

USER nzbidx
VOLUME /tmp

CMD ["sh", "-c", "python -m nzbidx_ingest & exec uvicorn nzbidx_api.main:app --host 0.0.0.0 --port 8080"]
