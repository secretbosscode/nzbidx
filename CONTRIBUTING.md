# Contributing

- Use issues and pull requests for changes.
- Format code with `black` and lint with `ruff`.
- Run tests for affected services before submitting a PR.

## Release checklist

1. Ensure the working tree is clean.
2. Run `ruff check .`, `black --check .` and `PYTHONPATH=. pytest`.
3. Execute `bash scripts/smoke.sh` against a fresh stack.
4. Run `pip-audit`.
5. Update `CHANGELOG.md` and set `VERSION` when invoking `make release VERSION=x.y.z`.
6. Verify the tag and pushes printed by the target.
