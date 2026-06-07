.PHONY: help install install-dev test coverage run clean format lint

help:
	@echo "Pulsar development commands:"
	@echo "  make install        Install package"
	@echo "  make install-dev    Install package with dev tools"
	@echo "  make test           Run tests"
	@echo "  make coverage       Generate coverage report"
	@echo "  make run            Run pulsar CLI (FILE=path/to/file)"
	@echo "  make format         Format code with black"
	@echo "  make lint           Lint code with ruff"
	@echo "  make clean          Remove build artifacts"

install:
	pip install -e .

install-dev:
	pip install -e ".[dev]"

test:
	pytest tests/ -v --tb=short

coverage:
	pytest tests/ --cov=pulsar --cov-report=html --cov-report=term-missing
	@echo "Coverage report generated in htmlcov/index.html"

run:
	@if [ -z "$(FILE)" ]; then \
		echo "Usage: make run FILE=path/to/file.csv"; \
	else \
		python -m pulsar.cli $(FILE); \
	fi

format:
	black pulsar tests

lint:
	ruff check pulsar tests

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".coverage" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "htmlcov" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true

.DEFAULT_GOAL := help