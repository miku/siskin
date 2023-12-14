# Makefile for siskin deployment
#
#     $ make deploy
#
SHELL := /bin/bash
ANSIBLE_OPTS = ANSIBLE_RETRY_FILES_ENABLED=false ANSIBLE_NOCOWS=true ANSIBLE_HOST_KEY_CHECKING=false

.PHONY: whoami
whoami: ## test connection
	$(ANSIBLE_OPTS) ansible-playbook -v -i hosts whoami222.yml

.PHONY: deploy
deploy: ## deploy to site
	$(ANSIBLE_OPTS) ansible-playbook -v -i hosts site.yml
