SHELL := /bin/bash

.PHONY: venv install install-dev run run-pre run-open fmt lint test

venv:
	python3 -m venv .venv

install:
	@source .venv/bin/activate && pip install -e .

install-dev:
	@source .venv/bin/activate && pip install -e .[dev]

run:
	@source .venv/bin/activate && OPEN=$(if $(PRE),0,$(OPEN)) PYTHONPATH=src python -m watchlist.cli

run-pre:
	@source .venv/bin/activate && OPEN=0 PYTHONPATH=src python -m watchlist.cli

run-open:
	@source .venv/bin/activate && OPEN=1 PYTHONPATH=src python -m watchlist.cli

fmt:
	@source .venv/bin/activate && black src tests

lint:
	@source .venv/bin/activate && ruff check src tests

test:
	@source .venv/bin/activate && pytest
