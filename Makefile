# Makefile for Lakevision development

.PHONY: help init-be run-be lint-be clean-be \
        init-fe run-fe build-fe preview-fe lint-fe clean-fe \
        run-all clean

help:
	@echo "Lakevision Makefile"
	@echo ""
	@echo "Backend:"
	@echo "  make init-be       - Create Python venv and install backend dependencies"
	@echo "  make run-be        - Run FastAPI backend with uvicorn"
	@echo "  make clean-be      - Remove backend virtualenv"
	@echo ""
	@echo "Frontend:"
	@echo "  make init-fe       - Install frontend dependencies"
	@echo "  make run-fe        - Run frontend dev server"
	@echo "  make build-fe      - Build production frontend"
	@echo "  make preview-fe    - Preview production frontend"
	@echo "  make lint-fe       - Lint frontend code"
	@echo "  make clean-fe      - Remove node_modules"
	@echo ""
	@echo "Combined:"
	@echo "  make clean         - Clean all dev artifacts"

# --- Backend ---

init-be:
	cd be && python -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt

run-be:
	cd be && set -a && source ../.env && set +a && PYTHONPATH=app .venv/bin/uvicorn app.api:app --reload --port 8000

clean-be:
	rm -rf be/.venv

# --- Frontend ---

init-fe:
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
