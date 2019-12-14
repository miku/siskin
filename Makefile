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
	rm -rf siskin.egg-info
	rm -rf build/ dist/ .tox/ .pytest_cache/
	find . -name "*.pyc" -exec rm -f {} \;
	find . -name ".DS_Store" -exec rm -f {} \;
	rm -f siskin.pex
	rm -f siskin.shiv
	rm -f .coverage
	rm -f tags

# Fix imports, requires https://github.com/timothycrosley/isort.
.PHONY: imports
imports:
	isort -rc --atomic .

# Basic scoring, requires https://www.pylint.org/.
.PHONY: pylint
pylint:
	pylint siskin

# Automatic code formatting, requires https://github.com/google/yapf.
.PHONY: style
style:
	yapf -p -i -r siskin

# Generate a PNG of the current update dependency tree, https://git.io/v5sdS.
docs/catalog/AIUpdate.png: $(PY_FILES)
	taskdeps-dot AIUpdate | dot -Tpng > $@

# Generate a SID list for reference.
docs/sids.tsv:
	curl -v "https://projekte.ub.uni-leipzig.de/projects/metadaten-quellen/wiki/SIDs.xml?key=$$REDMINE_API_KEY" | \
		xmlcutty -path /wiki_page/text | sed -e 's/"//g' | cut -d '|' -f2-5 | \
		awk -F '|' '{print $$1"\t"$$2"\t"$$3}' | tail -n +4 > $@

