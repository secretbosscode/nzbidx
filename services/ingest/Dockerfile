FROM python:3.11-slim

WORKDIR /app

# Install runtime dependencies
COPY pyproject.toml .
RUN pip install --no-cache-dir python-dotenv

# Copy application source
COPY src/ ./src

CMD ["python", "-m", "nzbidx_ingest.main"]
