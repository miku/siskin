# Might want to use /usr/local as well ...
PREFIX = /usr/local
DOLLAR = $$

HOSTNAME = $(shell hostname)
WHOAMI = $(shell whoami)
ACTIVATE = $(WORKON_HOME)/tasktree/bin/activate
help:
	@echo "make clean"

clean:
	rm -rf tsk.egg-info
	rm -rf build/ dist/

