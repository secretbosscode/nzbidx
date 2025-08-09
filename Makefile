.PHONY: up down logs test fmt lint

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

test:
	pytest

fmt:
        black .

lint:
        ruff check .
