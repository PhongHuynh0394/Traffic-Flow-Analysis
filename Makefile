include .env

service ?= none

# Group servies
airflow = airflow-webserver airflow-scheduler postgres airflow-triggerer airflow-init
kafka = kafka-broker-1
api = model-api
ALL_SERVICES = $(airflow) $(kafka) clickhouse
COMPOSE_FILE = docker-compose.yaml

.PHONY: build up down downall downing restart notebook

build:
	docker compose build --no-cache

up:
ifeq ($(service), airflow)
	@echo "Starting Airflow services..."
	docker-compose -f $(COMPOSE_FILE) up -d $(airflow)
else ifeq ($(service), kafka)
	@echo "Starting $(service) services..."
	docker-compose -f $(COMPOSE_FILE) up -d $(kafka)
else ifeq ($(service), none)
	@echo "Starting all services..."
	docker-compose -f $(COMPOSE_FILE) up -d $(ALL_SERVICES)
else
	@echo "Unknown class: $(service)"
	@echo "Supported classes: airflow, clickhouse, none (default for all services)"
	exit 1
endif

notebook:
	docker compose up -d notebook

downall:
	docker compose down --volumes --rmi all

downing:
	docker compose down --volumes && docker images --format "{{.Repository}}:{{.Tag}}" | grep -v postgres | xargs -I {} docker rmi -f {}

restart:
	docker compose restart

down:
	docker compose down
