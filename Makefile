.PHONY: format lint lint-fix build up up-dev down logs restart clean help test

format:
	ruff format .
	ruff check --fix .

lint:
	ruff check .
	ruff format --diff

TEST_FILE ?= tests/unit_tests

test:
	uv run pytest --disable-socket --allow-unix-socket $(TEST_FILE)

help:
	@echo "Available commands:"
	@echo "  make format    - Format code with ruff"
	@echo "  make lint      - Check code with ruff"
	@echo "  make lint-fix  - Fix linting issues with ruff"
	@echo "  make test      - Run unit tests"
	@echo "  make build     - Build Docker images"
	@echo "  make up        - Start all services in detached mode"
	@echo "  make up-dev    - Start all services with live reload"
	@echo "  make down      - Stop all services"
	@echo "  make logs      - View logs of all services"
	@echo "  make restart   - Restart all services"
	@echo "  make clean     - Remove containers, volumes, and images"

build:
	docker-compose build

up:
	docker-compose up -d

up-dev:
	docker-compose up

down:
	docker-compose down

logs:
	docker-compose logs -f

restart:
	docker-compose restart

clean:
	docker-compose down -v
	docker rmi langconnect-api:latest 2>/dev/null || true
