.PHONY: docker-build
docker-build:
	@docker-compose -f docker/docker-compose.yml build

.PHONY: docker-test
docker-test:
	@docker-compose -f docker/docker-compose.yml up

.PHONY: venv
venv:
	@poetry shell

.PHONY: test
test:
	@pytest -v

.PHONY: start-postgres
start-postgres:
	@docker compose -f docker/docker-compose-postgres.yml up --build -d

.PHONY: stop-postgres
stop-postgres:
	@docker compose -f docker/docker-compose-postgres.yml down

.PHONY: streaming-build
streaming-build:
	@docker compose -f streaming/docker-compose.yml build

.PHONY: streaming-up
streaming-up:
	@docker compose -f streaming/docker-compose.yml up -d

.PHONY: streaming-down
streaming-down:
	@docker compose -f streaming/docker-compose.yml down


.PHONY: trino-up
trino-up:
	@docker compose -f trino/docker-compose.yml up -d

.PHONY: trino-down
trino-down:
	@docker compose -f trino/docker-compose.yml down