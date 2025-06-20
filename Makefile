.PHONY: build
build:
	@docker compose -f docker/docker-compose.yml build

.PHONY: test
test:
	@docker compose -f docker/docker-compose-test.yml down
	@docker compose -f docker/docker-compose-test.yml up --exit-code-from pyspark_test_container

.PHONY: venv
venv:
	@poetry shell

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