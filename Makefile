SHELL := /bin/bash

help:
	@echo "make clean"

clean:
	rm -rf siskin.egg-info
	rm -rf build/ dist/
