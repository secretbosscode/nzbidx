.PHONY: compose-up compose-down logs test fmt lint seed-os

compose-up:
        docker compose up -d

compose-down:
        docker compose down

logs:
        docker compose logs -f

test:
        pytest

fmt:
        black .

lint:
        ruff check .

seed-os:
        python scripts/seed_os.py
