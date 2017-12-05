SHELL := /bin/bash
PY_FILES := $(shell find siskin -name \*.py -print)

dist:
	python setup.py sdist

upload:
	# https://pypi.python.org/pypi?%3Aaction=register_form
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

docs/catalog/AIUpdate.png: $(PY_FILES)
	taskdeps-dot AIUpdate | dot -Tpng > $@

