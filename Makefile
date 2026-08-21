.PHONY: help install install-postgres format lint type test test-migration-e2e lock build dist changelog release docs-build docs-serve clean all

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
		uv run coverage run -m pytest -v -m "not migration_e2e"
	uv run coverage combine
	uv run coverage report

test-xdist:  ## Reproduce CI's parallel configuration (no coverage gate -- see below)
	uv run pytest -n auto --dist loadgroup -m "not migration_e2e"

test-migration-e2e:  ## Run the real-uv/network migration-orchestrator e2e test (slow, not in `make all`)
	PYTEST_AIRFLOW_IN_A_BOX_MIGRATION_E2E=1 uv run pytest -v -m migration_e2e tests/migration/test_e2e.py

lock:  ## Verify the dependency lock is current
	uv lock --check

build:  ## Build the source and wheel distributions
	uv run python -m build --installer uv

dist:  ## Build a clean sdist + wheel and validate them with twine
	rm -rf dist
	uv run python -m build --installer uv
	uvx twine@7.0.0 check --strict dist/*

changelog:  ## Preview the assembled Unreleased section from changelog.d fragments
	uv run towncrier build --version Unreleased --draft

docs-build:  ## Build the documentation site, failing on broken links or nav entries
	uv run --group docs mkdocs build --strict

docs-serve:  ## Serve the documentation site locally with live reload
	uv run --group docs mkdocs serve

release:  ## Build the changelog from fragments, tag the current version, and print the gh release command (does not publish)
	@set -eu; \
	pyproject_version="$$(uv version --short --color never)"; \
	init_version="$$(sed -n 's/^__version__ = "\(.*\)"$$/\1/p' src/pytest_airflow_in_a_box/__init__.py)"; \
	if [ "$$pyproject_version" != "$$init_version" ]; then \
		echo "version mismatch: pyproject.toml=$$pyproject_version __init__.py=$$init_version" >&2; \
		exit 1; \
	fi; \
	if [ -n "$$(git status --porcelain -- . ':!changelog.d' ':!CHANGELOG.md')" ]; then \
		echo "working tree is not clean" >&2; \
		exit 1; \
	fi; \
	if [ -n "$$(find changelog.d -maxdepth 1 -name '*.md' ! -name 'README.md')" ]; then \
		uv run towncrier build --version "$$pyproject_version" --date "$$(date +%Y-%m-%d)" --yes; \
		git add CHANGELOG.md changelog.d; \
		git commit -m "Update CHANGELOG.md for v$$pyproject_version"; \
	fi; \
	tag="v$$pyproject_version"; \
	git tag -a "$$tag" -m "$$tag" && \
	git push origin "$$tag" && \
	echo "Tag $$tag pushed. Publish with:" && \
	echo "  gh release create $$tag --title $$tag --generate-notes"

clean:  ## Remove every git-ignored file and directory
	git clean -fdX

all: format lint type test lock build docs-build  ## Run the complete development pipeline
