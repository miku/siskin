SHELL := /bin/bash

dist:
	python setup.py sdist

upload:
	# https://pypi.python.org/pypi?%3Aaction=register_form
	# $ cat ~/.pypirc
	# [pypi]
	# username:abc
	# password:secret
	python setup.py sdist upload

clean:
	rm -rf siskin.egg-info
	rm -rf build/ dist/
	find . -name "*.pyc" -exec rm -f {} \;
