# Makefile for siskin. Project packaging and some code maintenance tasks.

SHELL := /bin/bash
PY_FILES := $(shell find siskin -name \*.py -print)

.PHONY: dist upload clean imports pylint style

# Create a source distribution.
dist:
	python setup.py sdist

# Upload requires https://github.com/pypa/twine.
upload: dist
	# https://pypi.org/account/register/
	# $ cat ~/.pypirc
	# [pypi]
	# username:abc
	# password:secret
	#
	# For internal repositories, name them in ~/.pypirc, then run: make upload
	# TWINE_OPTS="-r internal" to upload to hosted pypi repository.
	twine upload $(TWINE_OPTS) dist/*

clean:
	rm -rf siskin.egg-info
	rm -rf build/ dist/ .tox/ .pytest_cache/
	find . -name "*.pyc" -exec rm -f {} \;
	find . -name ".DS_Store" -exec rm -f {} \;
	rm -f siskin.pex
	rm -f siskin.shiv
	rm -f .coverage

# Fix imports, requires https://github.com/timothycrosley/isort.
imports:
	isort -rc --atomic .

# Basic scoring, requires https://www.pylint.org/.
pylint:
	pylint siskin

# Automatic code formatting, requires https://github.com/google/yapf.
style:
	yapf -p -i -r siskin

# Generate a PNG of the current update dependency tree, https://git.io/v5sdS.
docs/catalog/AIUpdate.png: $(PY_FILES)
	taskdeps-dot AIUpdate | dot -Tpng > $@

docs/sids.tsv:
	curl -v "https://projekte.ub.uni-leipzig.de/projects/metadaten-quellen/wiki/SIDs.xml?key=$$REDMINE_API_KEY" | xmlcutty -path /wiki_page/text | sed -e 's/"//g' | cut -d '|' -f2-5 | awk -F '|' '{print $$1"\t"$$2"\t"$$3}' | tail -n +4 > $@

# Experimental: build a single file executable for `taskdo` command.
siskin.pex:
	pex -v -r <(pip freeze) --disable-cache -c taskdo -o $@

# Experimental: build a single file executable for `taskdo` command.
siskin.shiv:
	shiv -c siskin -o $@ .

