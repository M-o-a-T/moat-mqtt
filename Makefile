#!/usr/bin/make -f

PACKAGE = moat-mqtt
MAKEINCL ?= $(shell python3 -mmoat src path)/make/py

ifneq ($(wildcard $(MAKEINCL)),)
include $(MAKEINCL)
# availabe via http://github.com/smurfix/sourcemgr

else
%:
	@echo "Please fix 'python3 -mmoat src path'."
	@exit 1
endif

