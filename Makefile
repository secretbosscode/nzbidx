.PHONY: build up down smoke logs prune prune-filetypes test fmt lint release

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

prune:
        scripts/prune_branches.sh

prune-filetypes:
	python scripts/prune_disallowed_filetypes.py

test:
	PYTHONPATH=. pytest

fmt:
	black .

lint:
        ruff check .

release:
	@[ -n "$(VERSION)" ] || (echo "VERSION required" && exit 1)
	@git diff --quiet || (echo "git tree dirty" && exit 1)
	@ruff check .
	@black --check .
        @PYTHONPATH=. pytest -q
	@bash scripts/smoke.sh
	# Ensure pip-audit is available
	@pip install pip-audit >/dev/null
	@pip-audit
	@DATE=$$(date +%Y-%m-%d); perl -0 -i -pe "s/^## \[Unreleased\]/## $(VERSION) ($$DATE)/" CHANGELOG.md
	@git tag v$(VERSION)
	@if [ -n "$$CI" ]; then \
		git push origin main; \
		git push origin v$(VERSION); \
	else \
		echo git push origin main; \
		echo git push origin v$(VERSION); \
	fi
