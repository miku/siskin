# Makefile for siskin deployment
#
# Temporary error that may occur: [172.26.53.111]: FAILED! => {"msg": "Timeout
# (12s) waiting for privilege escalation prompt: Warning: Permanently added
# '[127.0.0.1]:222' (ED25519) to the list of known hosts.\r\r\n\r\n"}

SHELL := /bin/bash
ANSIBLE_OPTS = ANSIBLE_RETRY_FILES_ENABLED=false ANSIBLE_NOCOWS=true ANSIBLE_HOST_KEY_CHECKING=false

.PHONY: whoami
whoami: ## test connection
	$(ANSIBLE_OPTS) ansible-playbook -v -i hosts whoami222.yml

.PHONY: deploy
deploy: ## deploy to site
	$(ANSIBLE_OPTS) ansible-playbook -v -i hosts site.yml
