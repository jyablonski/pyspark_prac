.PHONY: docker-build
docker-build:
	@docker-compose -f docker/docker-compose.yml build

.PHONY: docker-test
docker-test:
	@docker-compose -f docker/docker-compose.yml up

.PHONY: install-venv
install-venv:
	@pipenv install --dev

.PHONY: venv
venv:
	@pipenv shell

.PHONY: test
test:
	@pytest -v

.PHONY: start-postgres
start-postgres:
	@docker compose -f docker/docker-compose-postgres.yml up --build -d

.PHONY: stop-postgres
stop-postgres:
	@docker compose -f docker/docker-compose-postgres.yml down
