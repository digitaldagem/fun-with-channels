SRC_DOCKER_IMAGES = $(shell docker images -q fun-with-channels-src)
POSTGRES_DOCKER_IMAGES = $(shell docker images -q postgres)

up:
	docker-compose up --build --remove-orphans

down:
ifneq ($(strip $(SRC_DOCKER_IMAGES)),$(strip $(POSTGRES_DOCKER_IMAGES)))
	docker-compose down -v --remove-orphans
	docker rmi $(SRC_DOCKER_IMAGES)
	docker rmi $(POSTGRES_DOCKER_IMAGES)
endif

test:
	go test ./...

.PHONY: up down test