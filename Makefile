type-check:
	uv run mypy .
	uv run pyrefly check

format:
	uv run ruff format

lint:
	uv run ruff check

lint-fix:
	uv run ruff check --fix

test:
	uv run coverage run -m pytest
	uv run coverage report -m

check: test format lint type-check

.PHONY: type-check format lint lint-fix test check
