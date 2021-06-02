SHELL := /bin/bash

.PHONY: image
image:
	docker build -t lftplegacy .

.PHONY: clean
clean:
	docker rmi -f lftplegacy
