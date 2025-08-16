
.DEFAULT_GOAL := help
include .env

define HELP

Available commands:

- run: This will start core.

- pull: This will pull all the base images required, Note I use a custom image that has some Confluent Connectors installed on top the base images.

- stop: Stop the project.
- start: Start a stopped project.
- down: Tear down the project, clean directories.
- ps: Show all running containers.

- logs: Show/tail logs.
- logsf: Stream logs.
- watch: Watch logs.


endef

export HELP
help:
	@echo "$$HELP"
.PHONY: help


#	docker rmi $(docker images -q --filter "dangling=true")

# run:
# 	docker compose -f docker-compose.yml -p graph up -d \
# 		--remove-orphans 

run:
	docker compose -f docker-compose.yml -p graph up -d \
		broker schema-registry control-center connect \
		neo4j mongodb postgrescdc redis \
		--remove-orphans 

pull: 
	docker pull confluentinc/cp-kafka:7.9.1
	docker pull confluentinc/cp-schema-registry:7.9.1
	docker pull confluentinc/cp-enterprise-control-center:7.9.1
	docker pull confluentinc/cp-kafka-connect-base:7.7.1
	docker pull mongodb/mongodb-atlas-local:latest
	docker pull redis/redis:8.0-bookworm
	docker pull postgres:12


 # https://neo4j.com/docs/operations-manual/current/docker/operations/#docker-neo4j-plugins
	
createtopics:
	./creTopics/creTopics.sh

stop:
	docker compose stop

start:
	docker compose start

down:	
	docker compose down 
	cd data/; rm -rf confluent.d
	cd data/; rm -rf neo4j_db
	cd data/; rm -rf postgrescdc
	cd data/; rm -rf mongodb
	cd data/; rm -rf redis

ps:
	docker compose ps

logs:
	docker compose logs

logsf:
	docker compose logs -f

watch:
	watch docker compose ps
