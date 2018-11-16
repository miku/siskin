SHELL := /bin/bash
PY_FILES := $(shell find siskin -name \*.py -print)

dist:
	python setup.py sdist

upload:
	# https://pypi.org/account/register/
	# $ cat ~/.pypirc
	# [pypi]
	# username:abc
	# password:secret
	python setup.py sdist
	twine upload dist/*

imports:
	isort -rc --atomic .

pylint:
	pylint siskin

clean:
	rm -rf siskin.egg-info
	rm -rf build/ dist/
	find . -name "*.pyc" -exec rm -f {} \;
	find . -name ".DS_Store" -exec rm -f {} \;
	rm -f siskin.pex

docs/catalog/AIUpdate.png: $(PY_FILES)
	taskdeps-dot AIUpdate | dot -Tpng > $@

# Experimental: build a single file executable for `taskdo` command.
siskin.pex:
	pex -v -r <(pip freeze) --disable-cache -c taskdo -o $@
