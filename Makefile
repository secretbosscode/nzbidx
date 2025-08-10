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
	PYTHONPATH=. pytest

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
	@ruff check .
	@black --check .
	@PYTHONPATH=. pytest -q services/api/tests services/ingest/tests
	@bash scripts/smoke.sh
	@pip install pip-audit >/dev/null
	@pip-audit --fail-on ${PIP_AUDIT_LEVEL:-high}
	@DATE=$$(date +%Y-%m-%d); perl -0 -i -pe "s/^## \[Unreleased\]/## $(VERSION) ($$DATE)/" CHANGELOG.md
	@git tag v$(VERSION)
	@if [ -n "$$CI" ]; then \
		git push origin main; \
		git push origin v$(VERSION); \
	else \
		echo git push origin main; \
		echo git push origin v$(VERSION); \
	fi
