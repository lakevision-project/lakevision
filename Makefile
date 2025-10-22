SHELL := /bin/bash

VENV_DIR=be/.venv
VENV_PYTHON=$(VENV_DIR)/bin/python

.PHONY: help \
        init-fe run-fe build-fe preview-fe lint-fe clean-fe \
        run-all clean

help:
	@echo "Lakevision Makefile"
	@echo ""
	@echo "Backend:"
	@echo "  make init-be.          - Create Python venv and install backend dependencies"
	@echo "  make sample-catalog    - Populate catalog with demo data"
	@echo "  make run-be            - Run FastAPI backend with uvicorn"
	@echo "  make clean-be          - Remove backend virtualenv"
	@echo "  make clean-catalog     - Remove sample catalog and warehouse files"
	@echo ""
	@echo ""
	@echo "Scheduler:"
	@echo "  make run-scheduler     - Run scheduler app"
	@echo "Frontend:"
	@echo "  make init-fe           - Install frontend dependencies"
	@echo "  make run-fe            - Run frontend dev server"
	@echo "  make build-fe          - Build production frontend"
	@echo "  make preview-fe        - Preview production frontend"
	@echo "  make lint-fe           - Lint frontend code"
	@echo "  make clean-fe          - Remove node_modules"
	@echo ""
	@echo "Combined:"
	@echo "  make clean             - Clean all dev artifacts"

# --- Backend ---

CHECK_VENV = \
	@if [ ! -f "$(VENV_PYTHON)" ]; then \
	  echo "🚨 Virtual environment not found at $(VENV_PYTHON). Run 'make init-be' first."; \
	  exit 1; \
	fi

.PHONY: init-be
init-be:
	cd be && python -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt -r requirements-dev.txt


.PHONY: run-be
run-be:
	$(CHECK_VENV)
	cd be && set -a && source ../.env && set +a && PYTHONPATH=app ../$(VENV_PYTHON) -m uvicorn app.api:app --reload --port 8000

.PHONY: clean-be
clean-be:
	rm -rf be/.venv

.PHONY: sample-catalog-deps
sample-catalog-deps:
	$(CHECK_VENV)
	@echo "📚 Ensuring SQLite extras for PyIceberg are available (sample only)..."
	@cd be && ../$(VENV_PYTHON) -m pip install "pyiceberg[sql-sqlite]"

.PHONY: sample-catalog
sample-catalog: sample-catalog-deps
	$(CHECK_VENV)
	@echo "📦 Creating sample in-memory Iceberg catalog with demo data..."
	cd be && set -a && source ../.env && set +a && PYTHONPATH=app ../$(VENV_PYTHON) ../scripts/load_sample_data.py

.PHONY: clean-catalog
clean-catalog:
	@echo "🧹 Removing sample catalog and warehouse files..."
	rm -rf be/warehouse/

.PHONY: test-be
test-be:
	$(CHECK_VENV)
	cd be && PYTHONPATH=app ../$(VENV_PYTHON) -m pytest tests

# --- Scheduler ---

.PHONY: run-scheduler
run-scheduler:
	$(CHECK_VENV)
	cd be && set -a && source ../.env && set +a && PYTHONPATH=app ../$(VENV_PYTHON) -m app.scheduler

# --- Frontend ---

.PHONY: check-npm
check-npm:
	@command -v npm >/dev/null 2>&1 || { \
		echo "❌ npm is required but not installed."; \
		echo ""; \
		echo "Install Node.js (which includes npm):"; \
		echo "  macOS:   brew install node"; \
		echo "  Ubuntu:  sudo apt-get update && sudo apt-get install -y nodejs npm"; \
		echo "  Fedora:  sudo dnf install -y nodejs npm"; \
		echo "  Windows: Download from https://nodejs.org/"; \
		exit 1; }

init-fe: check-npm
	cd fe && npm install

prepare-fe-env:
	mkdir -p fe
	> fe/.env
	while IFS= read -r line || [[ -n "$$line" ]]; do \
	    if [[ -z "$$line" || "$$line" == \#* ]]; then \
	        echo "$$line" >> fe/.env; \
	        continue; \
	    fi; \
	    key=$$(echo "$$line" | awk -F'=' '{print $$1}' | xargs); \
	    if [[ "$$key" == PUBLIC_* || "$$key" == VITE_* ]]; then \
	        echo "$$line" >> fe/.env; \
	    fi; \
	done < .env

run-fe: prepare-fe-env
	cd fe && npm run dev -- --port 8081

build-fe:
	cd fe && npm run build

preview-fe:
	cd fe && npm run preview

lint-fe:
	cd fe && npm run lint

clean-fe:
	rm -rf fe/node_modules fe/.svelte-kit

# --- Combined ---

clean: clean-be clean-fe
