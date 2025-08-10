.PHONY: build up down smoke logs test fmt lint seed-os snapshot-repo

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down -v

smoke:
	bash scripts/smoke.sh

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

snapshot-repo:
	docker compose exec api python scripts/os_snapshot_repo.py
