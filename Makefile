.PHONY: install
install: ## Install all dependencies
	uv sync --all-groups
	uv run --dev pre-commit install

.PHONY: lint
lint: ## Run linting and fix issues
	uv run --dev ruff check --fix src
	uv run --dev ruff format src
	uv run --dev mypy src
