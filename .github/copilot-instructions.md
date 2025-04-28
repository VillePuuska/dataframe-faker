When making any code changes:
- You SHOULD add unit tests for new units, whenever sensible.
- You MUST use provided make-commands to run anything in the terminal.
  - Ask to add new commands if the existing ones are not sufficient.
- You MUST verify formatting.
- You MUST verify linting.
- You MUST verify type checking.
- You SHOULD avoid unnecessary simple comments.
  - Try to only comment parts of code where the reason for having the code or the way of implementing something is not obvious.

This project uses
- Python as the only programming language,
- uv to manage dependencies and the project environment,
- pytest to define and run tests,
- ruff to format and lint,
- mypy for static type analysis.
