# Developer entry points. Run `make help` for the list.
.DEFAULT_GOAL := help
PYTHON := python

.PHONY: help install lint format typecheck test pipeline-dev clean

help: ## Show this help.
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

install: ## Install the package with dev dependencies + pre-commit hooks.
	$(PYTHON) -m pip install -e ".[dev]"
	pre-commit install

lint: ## Run ruff lint checks.
	ruff check src tests

format: ## Auto-format with black + ruff.
	black src tests
	ruff check --fix src tests

typecheck: ## Run mypy static type checks.
	mypy src

test: ## Run the unit test suite with coverage.
	pytest

pipeline-dev: ## Run the full medallion pipeline locally against ./data.
	TOKYO_ENV=dev $(PYTHON) -m tokyo_olympics.pipeline --env dev

clean: ## Remove build/test/Spark local artifacts.
	rm -rf build dist *.egg-info .pytest_cache .ruff_cache .mypy_cache \
		htmlcov .coverage coverage.xml spark-warehouse metastore_db derby.log \
		data/bronze data/silver data/gold
