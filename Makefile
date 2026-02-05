# Makefile for siskin. Project packaging and some code maintenance tasks.

SHELL := /bin/bash
PY_FILES := $(shell find siskin -name \*.py -print)

# Create a source distribution.
.PHONY: dist
dist:
	# was: python setup.py sdist
	uv build

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
	# TODO: move to `uv publish`

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

.PHONY: test
test:
	# pip install pytest-cov
	pytest --cov .

# Update version across all files. Usage: make setversion V=2.1.0
.PHONY: setversion
setversion:
	@test -n "$(V)" || { echo "usage: make setversion V=x.y.z"; exit 1; }
	sed -i "s/^version = .*/version = '$(V)'/" pyproject.toml
	sed -i "s/^siskin_vendor_version_siskin: .*/siskin_vendor_version_siskin: $(V)/" ansible/roles/siskin/defaults/main.yml
	@echo "version set to $(V) in pyproject.toml and ansible/roles/siskin/defaults/main.yml"
