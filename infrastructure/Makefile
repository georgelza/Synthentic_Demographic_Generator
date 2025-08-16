
.DEFAULT_GOAL := help
include .env

define HELP

Available commands:

- pull: This will pull all the base images required, Note I use a custom image that has some Confluent Connectors installed on top the base images.

- list: List all images

endef

export HELP
help:
	@echo "$$HELP"
.PHONY: help

pull: 
	docker pull confluentinc/cp-kafka:7.9.1
	docker pull confluentinc/cp-schema-registry:7.9.1
	docker pull confluentinc/cp-enterprise-control-center:7.9.1
	docker pull confluentinc/cp-kafka-connect-base:7.7.1
	docker pull mongodb/mongodb-atlas-local:latest
	docker pull redis/redis:8.0-bookworm
	docker pull postgres:12

list:
	docker images