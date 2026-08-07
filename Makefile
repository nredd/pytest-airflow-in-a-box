.PHONY: help install format lint type test lock build clean all

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install:  ## Install the locked development environment and git hooks
	uv sync
	uv run prek install

format:  ## Format code with ruff
	uv run ruff format .

lint:  ## Lint and fix code with ruff
	uv run ruff check . --fix

type:  ## Type check source, tests, and scripts with ty
	uv run ty check

test:  ## Run tests with branch coverage, subprocess runs included
	uv run python scripts/install_coverage_pth.py
	COVERAGE_PROCESS_START=$(CURDIR)/pyproject.toml COVERAGE_FILE=$(CURDIR)/.coverage \
		uv run coverage run -m pytest -v
	uv run coverage combine
	uv run coverage report

lock:  ## Verify the dependency lock is current
	uv lock --check

build:  ## Build the source and wheel distributions
	uv run python -m build --installer uv

clean:  ## Remove every git-ignored file and directory
	git clean -fdX

all: format lint type test lock build  ## Run the complete development pipeline
