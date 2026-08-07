.PHONY: help install format lint type test lock build clean all

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install:  ## Install the locked development environment
	uv sync

format:  ## Format code with ruff
	uv run ruff format .

lint:  ## Lint and fix code with ruff
	uv run ruff check . --fix

type:  ## Type check source, tests, and scripts with ty
	uv run ty check

test:  ## Run tests with complete branch coverage
	uv run coverage run -m pytest -v
	uv run coverage report

lock:  ## Verify the dependency lock is current
	uv lock --check

build:  ## Build the source and wheel distributions
	uv run python -m build --installer uv

clean:  ## Remove generated development artifacts
	rm -rf .coverage .pytest_cache .ruff_cache .tox .venv-ci dist build
	find . -type d -name __pycache__ -exec rm -rf {} +

all: format lint type test lock build  ## Run the complete development pipeline
