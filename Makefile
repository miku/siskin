# Makefile for siskin. Project packaging and some code maintenance tasks.

SHELL := /bin/bash
PY_FILES := $(shell find siskin -name \*.py -print)

.PHONY: dist upload clean imports pylint style

# Create a source distribution.
dist:
	python setup.py sdist

# Upload requires https://github.com/pypa/twine.
upload:
	# https://pypi.org/account/register/
	# $ cat ~/.pypirc
	# [pypi]
	# username:abc
	# password:secret
	python setup.py sdist
	twine upload dist/*

clean:
	rm -rf siskin.egg-info
	rm -rf build/ dist/ .tox/
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

# Experimental: build a single file executable for `taskdo` command.
siskin.pex:
	pex -v -r <(pip freeze) --disable-cache -c taskdo -o $@

# Experimental: build a single file executable for `taskdo` command.
siskin.shiv:
	shiv -c siskin -o $@ .

