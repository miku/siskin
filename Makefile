SHELL := /bin/bash

dist:
	python setup.py sdist

upload:
	python setup.py sdist upload

clean:
	rm -rf siskin.egg-info
	rm -rf build/ dist/
	find . -name "*.pyc" -exec rm -f {} \;
