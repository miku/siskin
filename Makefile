# Makefile for siskin. Project packaging and some code maintenance tasks.

SHELL := /bin/bash
PY_FILES := $(shell find siskin -name \*.py -print)

# Create a source distribution.
.PHONY: dist
dist:
	python setup.py sdist

# Upload requires https://github.com/pypa/twine and some configuration.
.PHONY: upload
upload: dist
	# https://pypi.org/account/register/
	# $ cat ~/.pypirc
	# [pypi]
	# username:abc
	# password:secret
	#
	# For internal repositories, name them in ~/.pypirc (e.g. "internal"), then
	# run: make upload TWINE_OPTS="-r internal" to upload to hosted pypi
	# repository.
	#
	# For automatic package deployments, also see: .gitlab-ci.yml.
	twine upload $(TWINE_OPTS) dist/*

.PHONY: clean
clean:
	find . -name "*.pyc" -exec rm -f "{}" +
	find . -name ".DS_Store" -exec rm -f "{}" +
	find . -name "__pycache__" -exec rm -rf "{}" +
	find . -type d -name ".ipynb_checkpoints" -exec rm -rf "{}" +
	rm -f .coverage tags
	rm -rf build/ dist/ .tox/ .pytest_cache/
	rm -rf logs # Probably automatically created by some Java MAB library.
	rm -rf siskin.egg-info
	rm -rf .ruff_cache/

.PHONY: fmt
fmt:
	ruff format siskin

.PHONY: lint
lint:
	ruff check siskin

