SHELL := /bin/sh

# Config
DOCKER_COMPOSE ?= docker compose
BACKEND_DIR := backend
FRONTEND_DIR := frontend
COMPOSE_FILE := docker-compose.yml
API ?= http://localhost:5001
# Example GTFS URL (override with: make import GTFS_URL=...)
GTFS_URL ?=
PYTHON ?= python3
NPM ?= npm

.PHONY: help backend-install backend-test frontend-install frontend-test ci \
	compose-up compose-down compose-logs health initdb seed import \
	docker-build-backend docker-build-frontend docker-build

help: ## Show available targets
	@echo "\nUsage: make <target> [VAR=value]\n"
	@awk 'BEGIN {FS = ":.*##"}; /^[a-zA-Z0-9_-]+:.*##/ {printf "  %-24s %s\n", $$1, $$2}' $(MAKEFILE_LIST) | sort
	@echo "\nVariables:"
	@echo "  API=<url>       Backend API base (default: $(API))"
	@echo "  GTFS_URL=<url>  GTFS ZIP to import"

backend-install: ## Install backend dependencies
	cd $(BACKEND_DIR) && $(PYTHON) -m pip install --upgrade pip && $(PYTHON) -m pip install -r requirements.txt

backend-test: ## Run backend tests (pytest)
	cd $(BACKEND_DIR) && DATABASE_URL=sqlite:///local-tests.db FLASK_ENV=testing ALLOW_INITDB=true $(PYTHON) -m pytest -q --maxfail=1 --disable-warnings

frontend-install: ## Install frontend dependencies
	cd $(FRONTEND_DIR) && $(NPM) ci

frontend-test: ## Run frontend test suite (Jest)
	cd $(FRONTEND_DIR) && CI=true $(NPM) test -- --watchAll=false

ci: ## Install deps and run backend and frontend tests
	$(MAKE) backend-install
	$(MAKE) backend-test
	$(MAKE) frontend-install
	$(MAKE) frontend-test

compose-up: ## Build and start full stack in background
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) up --build -d

compose-down: ## Stop stack and remove containers
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) down

compose-logs: ## Tail logs from all services
	$(DOCKER_COMPOSE) -f $(COMPOSE_FILE) logs -f backend frontend db

health: ## Check backend health endpoint
	@curl -sS $(API)/health | jq . || curl -sS $(API)/health

initdb: ## Initialize database schema (requires server with ALLOW_INITDB=true)
	curl -X POST "$(API)/initdb"

seed: ## Import development seed data
	curl -X POST "$(API)/import/seed"

import: ## Import a GTFS feed via URL (set GTFS_URL=<url>)
	@test -n "$(GTFS_URL)" || (echo "Error: provide GTFS_URL=<url>" && exit 1)
	curl -X POST "$(API)/import/gtfs?url=$(GTFS_URL)"

# Optional: local image builds

docker-build-backend: ## Build backend Docker image (local tag)
	docker build -t obrail/backend:local $(BACKEND_DIR)

docker-build-frontend: ## Build frontend Docker image (local tag)
	docker build -t obrail/frontend:local $(FRONTEND_DIR)

docker-build: ## Build both backend and frontend images
	$(MAKE) docker-build-backend && $(MAKE) docker-build-frontend
