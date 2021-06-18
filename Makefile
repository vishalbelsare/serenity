# all our targets are phony (no files to check).
.PHONY: help build build-base tag-testing tag-prod prune publish

BASE_RELEASE=localhost:5000/cloudwallcapital/serenity-base:latest
BASE_BUILD_NUMBER_FILE=base-build-number.txt
BUILD_NUMBER_FILE=build-number.txt
DATE_TAG=`date +%Y.%m.%d`
BASE_BUILD_TAG=$(DATE_TAG)-b`cat $(BUILD_NUMBER_FILE)`
BUILD_TAG=$(DATE_TAG)-b`cat $(BUILD_NUMBER_FILE)`
BASE_IMAGE_NAME=cloudwallcapital/serenity-base:$(BASE_BUILD_TAG)
IMAGE_NAME=cloudwallcapital/serenity:$(BUILD_TAG)

help:
	@echo ''
	@echo 'Usage: make [TARGET] [EXTRA_ARGUMENTS]'
	@echo 'Targets:'
	@echo '  build    	build Docker image'
	@echo '  prune    	shortcut for docker system prune -af; cleans up inactive containers and cache'
	@echo ''

base-build:
	@echo $$(($$(cat $(BASE_BUILD_NUMBER_FILE)) + 1)) > $(BASE_BUILD_NUMBER_FILE)
	docker build --no-cache -t $(BASE_IMAGE_NAME) -f kubernetes/dockerfiles/serenity-base/Dockerfile .
	docker image tag $(BASE_IMAGE_NAME) cloudwallcapital/serenity-base:latest

build:
	@echo $$(($$(cat $(BUILD_NUMBER_FILE)) + 1)) > $(BUILD_NUMBER_FILE)
	docker build --build-arg BASE_IMAGE_NAME=$(BASE_RELEASE) --no-cache -t $(IMAGE_NAME) -f kubernetes/dockerfiles/serenity/Dockerfile .
	docker image tag $(IMAGE_NAME) cloudwallcapital/serenity:latest

base-push-latest:
	docker push cloudwallcapital/serenity-base:latest

base-push-testing:
	docker image tag $(BASE_IMAGE_NAME) cloudwallcapital/serenity-base:testing
	docker push cloudwallcapital/serenity-base:latest

base-push-prod:
	docker image tag $(BASE_IMAGE_NAME) cloudwallcapital/serenity-base:prod
	docker push cloudwallcapital/serenity-base:latest

base-push-local:
	docker image tag $(BASE_IMAGE_NAME) localhost:5000/cloudwallcapital/serenity-base:latest
	docker push localhost:5000/cloudwallcapital/serenity-base:latest

push-latest:
	docker push cloudwallcapital/serenity:latest

push-testing:
	docker image tag $(IMAGE_NAME) cloudwallcapital/serenity:testing
	docker push cloudwallcapital/serenity:testing

push-prod:
	docker image tag $(IMAGE_NAME) cloudwallcapital/serenity:prod
	docker push cloudwallcapital/serenity:prod

push-local:
	docker image tag $(IMAGE_NAME) localhost:5000/cloudwallcapital/serenity:latest
	docker push localhost:5000/cloudwallcapital/serenity:latest

prune:
	# clean all that is not actively used
	docker system prune -af

publish:
	python3 setup.py sdist bdist_wheel
	twine upload dist/*
	rm -rf build dist .egg serenity-trading.egg-info
