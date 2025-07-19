.PHONY: build up down logs clean test lint format

build: ## Build Docker containers
    docker compose build

up: ## Start all services
    docker compose up -d

down: ## Stop all services
    docker compose down

logs: ## Show logs for all services
    docker compose logs -f

logs-backend: ## Show backend logs
    docker compose logs -f backend

logs-frontend: ## Show frontend logs
    docker compose logs -f frontend

clean:
    docker compose down -v --remove-orphans
    docker system prune -f

restart: ## Restart all services
    docker compose restart

restart-backend: ## Restart backend service
    docker compose restart backend

restart-frontend: ## Restart frontend service
    docker compose restart frontend

shell-backend: ## Access backend container shell
    docker compose exec backend bash

shell-frontend: ## Access frontend container shell
    docker compose exec frontend bash

test: ## Run tests
    docker compose exec backend pytest

lint: ## Run linting
    docker compose exec backend flake8 app/

format: ## Format code
    docker compose exec backend black app/

health: ## Check service health
    curl -f http://localhost:8000/health
    curl -f http://localhost:8501

setup:
    cp .env.example .env
    @echo "Please edit .env file with your configuration"
    @echo "Then run: make build && make up"

dev-backend: ## Run backend in development mode
    cd backend && uvicorn main:app --reload --host 0.0.0.0 --port 8000

dev-frontend: ## Run frontend in development mode
    cd frontend && streamlit run app.py --server.port 8501

install-dev: ## Install development dependencies
    cd backend && pip install -r requirements.txt
    cd frontend && pip install -r requirements.txt