.PHONY: docker-build
docker-build:
	@docker-compose -f docker/docker-compose.yml build

.PHONY: docker-test
docker-test:
	@docker-compose -f docker/docker-compose.yml up