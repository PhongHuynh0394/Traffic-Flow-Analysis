include .env

SERVICE ?= none

# Group servies
AIRFLOW_SERVICES = airflow-webserver airflow-scheduler postgres airflow-triggerer airflow-init
# KAFKA_SERVICES = kafka
# CLICKHOUSE_SERVICES = clickhouse
ALL_SERVICES = $(AIRFLOW_SERVICES) 
COMPOSE_FILE = docker-compose.yaml

.PHONY: build up down downall downing restart 

build:
	docker compose build --no-cache

up:
ifeq ($(SERVICE), airflow)
	@echo "Starting Airflow services..."
	docker-compose -f $(COMPOSE_FILE) up -d $(AIRFLOW_SERVICES)
# else ifeq ($(SERVICE), someservice)
# 	@echo "Starting $(SERVICE) services..."
# 	docker-compose -f $(COMPOSE_FILE) up -d $(SERVICE_NAME)
else ifeq ($(SERVICE), none)
	@echo "Starting all services..."
	docker-compose -f $(COMPOSE_FILE) up -d $(ALL_SERVICES)
else
	@echo "Unknown class: $(SERVICE)"
	@echo "Supported classes: airflow, clickhouse, none (default for all services)"
	exit 1
endif

downall:
	docker compose down --volumes --rmi all

downing:
	docker compose down --volumes && docker images --format "{{.Repository}}:{{.Tag}}" | grep -v postgres | xargs -I {} docker rmi -f {}

restart:
	docker compose restart

down:
	docker compose down
