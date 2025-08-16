FROM python:3.12-slim

RUN addgroup --system --gid 1000 nzbidx && \
    adduser --system --uid 1000 --gid 1000 nzbidx

WORKDIR /app

# Use stdlib json by default for compatibility; set to 0 to enable orjson
ARG NZBIDX_USE_STD_JSON=1
ENV NZBIDX_USE_STD_JSON=${NZBIDX_USE_STD_JSON}

COPY services/api/ services/api/
# Schema file required by the API when applying migrations
COPY db/init/schema.sql services/api/src/db/init/schema.sql

# Install service dependencies from the pyproject.toml so the
# PostgreSQL driver is included in the final image.
RUN pip install --no-cache-dir -e services/api

COPY nzbidx_common/ nzbidx_common

USER nzbidx
VOLUME /tmp

CMD ["uvicorn", "nzbidx_api.main:app", "--host", "0.0.0.0", "--port", "8080", "--loop", "asyncio", "--http", "h11"]
