.PHONY: build up down smoke logs test fmt lint seed-os snapshot-repo release

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

release:
	@[ -n "$(VERSION)" ] || (echo "VERSION required" && exit 1)
	@git diff --quiet || (echo "git tree dirty" && exit 1)
	@DATE=$$(date +%Y-%m-%d); perl -0 -i -pe "s/## \[Unreleased\]/## [Unreleased]\n\n## [$(VERSION)] - $$DATE/" CHANGELOG.md
	@tag=v$(VERSION); git tag $$tag; echo "git tag $$tag"
