.PHONY: help install install-postgres format lint type test lock build dist release docs-build docs-serve clean all

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install:  ## Install the locked development environment and git hooks
	uv sync
	uv run prek install

install-postgres:  ## Install the development environment with the Postgres backend and hooks
	uv sync --extra postgres
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

dist:  ## Build a clean sdist + wheel and validate them with twine
	rm -rf dist
	uv run python -m build --installer uv
	uvx twine@7.0.0 check --strict dist/*

docs-build:  ## Build the documentation site, failing on broken links or nav entries
	uv run --group docs mkdocs build --strict

docs-serve:  ## Serve the documentation site locally with live reload
	uv run --group docs mkdocs serve

release:  ## Tag the current version and print the gh release command (does not publish)
	@pyproject_version="$$(uv version --short)"; \
	init_version="$$(sed -n 's/^__version__ = "\(.*\)"$$/\1/p' src/pytest_airflow_in_a_box/__init__.py)"; \
	if [ "$$pyproject_version" != "$$init_version" ]; then \
		echo "version mismatch: pyproject.toml=$$pyproject_version __init__.py=$$init_version" >&2; \
		exit 1; \
	fi; \
	if [ -n "$$(git status --porcelain)" ]; then \
		echo "working tree is not clean" >&2; \
		exit 1; \
	fi; \
	tag="v$$pyproject_version"; \
	git tag -a "$$tag" -m "$$tag"; \
	git push origin "$$tag"; \
	echo "Tag $$tag pushed. Publish with:"; \
	echo "  gh release create $$tag --title $$tag --generate-notes"

clean:  ## Remove every git-ignored file and directory
	git clean -fdX

all: format lint type test lock build docs-build  ## Run the complete development pipeline
