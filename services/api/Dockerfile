FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir fastapi uvicorn[standard] orjson

# Copy application source
COPY src/ .

CMD ["uvicorn", "nzbidx_api.main:app", "--host", "0.0.0.0", "--port", "8080"]
