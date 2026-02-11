# Makefile for siskin deployment
#
#     $ make deploy
#
SHELL := /bin/bash
ANSIBLE_OPTS = ANSIBLE_RETRY_FILES_ENABLED=false ANSIBLE_NOCOWS=true ANSIBLE_HOST_KEY_CHECKING=false
ANSIBLE_EXEC = uvx --from ansible-core ansible-playbook
EXTRA_VARS =

ifdef SISKIN_SOLR
EXTRA_VARS += -e siskin_solr=$(SISKIN_SOLR)
endif

.PHONY: whoami
whoami: ## test connection
	$(ANSIBLE_OPTS) $(ANSIBLE_EXEC) -v -i hosts whoami222.yml

.PHONY: deploy
deploy: ## deploy to site
	$(ANSIBLE_OPTS) $(ANSIBLE_EXEC) -v -i hosts site.yml $(EXTRA_VARS)
