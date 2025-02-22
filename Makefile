type-check:
	uv run mypy .

lint:
	uv run ruff check

lint-fix:
	uv run ruff check --fix

test:
	uv run pytest

check: test lint type-check

.PHONY: type-check lint lint-fix test check
