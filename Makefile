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

install:
	mkdir -p $(PREFIX)/usr/lib/moat/mqtt
	mkdir -p $(PREFIX)/usr/lib/sysusers.d
	mkdir -p $(PREFIX)/lib/systemd/system
	cp systemd/*.service $(PREFIX)/lib/systemd/system/
	cp systemd/sysusers $(PREFIX)/usr/lib/sysusers.d/moat-mqtt.conf

