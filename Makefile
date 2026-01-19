SHELL := /bin/bash

.PHONY: venv install install-dev run fmt lint test

venv:
	python3 -m venv .venv

install:
	@source .venv/bin/activate && pip install -e .

install-dev:
	@source .venv/bin/activate && pip install -e .[dev]

run:
	@source .venv/bin/activate && watchlist-build

fmt:
	@source .venv/bin/activate && black src tests

lint:
	@source .venv/bin/activate && ruff check src tests

test:
	@source .venv/bin/activate && pytest
