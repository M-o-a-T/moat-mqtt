#!/usr/bin/make -f

.PHONY: doc test update all tag pypi upload

all:
		@echo "Please use 'python setup.py'."
		@exit 1

# need to use python3 sphinx-build
PATH := /usr/share/sphinx/scripts/python3:${PATH}

PACKAGE = distmqtt
PYTHON ?= python3
export PYTHONPATH=$(shell pwd)

PYTEST ?= ${PYTHON} -mpytest
TEST_OPTIONS ?= -xvvv --full-trace

BUILD_DIR ?= build
INPUT_DIR ?= docs

# Sphinx options (are passed to build_docs, which passes them to sphinx-build)
#   -W       : turn warning into errors
#   -a       : write all files
#   -b html  : use html builder
#   -i [pat] : ignore pattern

SPHINXOPTS ?= -a -W -b html
AUTOSPHINXOPTS := -i *~ -i *.sw* -i Makefile*

SPHINXBUILDDIR ?= $(BUILD_DIR)/sphinx/html
ALLSPHINXOPTS ?= -d $(BUILD_DIR)/sphinx/doctrees $(SPHINXOPTS) docs

test:
	-rm -rf obj
	mkdir obj
	env PYTHONPATH=obj python3 setup.py develop -d obj
	PYTHONPATH=obj $(PYTEST) $(TEST_OPTIONS) tests

doc:
	sphinx3-build -a $(INPUT_DIR) $(BUILD_DIR)

livehtml: docs
	sphinx-autobuild $(AUTOSPHINXOPTS) $(ALLSPHINXOPTS) $(SPHINXBUILDDIR)



tagged:
	git describe --tags --exact-match
	test $$(git ls-files -m | wc -l) = 0

pypi:   tagged
	python3 setup.py sdist upload


