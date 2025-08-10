FROM python:3.11-slim

RUN addgroup --system --gid 1000 nzbidx && \
    adduser --system --uid 1000 --gid 1000 nzbidx

WORKDIR /app

COPY pyproject.toml .
RUN pip install --no-cache-dir python-dotenv

COPY src/ ./src

USER nzbidx
VOLUME /tmp

CMD ["python", "-m", "nzbidx_ingest.main"]
