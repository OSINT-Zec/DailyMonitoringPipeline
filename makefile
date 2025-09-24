# Makefile

SHELL := /bin/sh
.ONESHELL:
.SILENT:

# -------- Variables --------
VENV := .venv
PY := $(VENV)/bin/python
PIP := $(VENV)/bin/pip

REQ_FULL := requirements.txt
REQ_LIGHT := requirements-lowarm.txt

export PIP_DISABLE_PIP_VERSION_CHECK=1

# -------- Phony targets --------
.PHONY: help venv install-light install-full upgrade-pip initdb collect enrich cluster summarize digest all clean

help:
	printf "%s\n" \
	"Targets:" \
	"  make venv            # create virtualenv" \
	"  make install-light   # install light deps (e.g., low/arm)" \
	"  make install-full    # install full deps" \
	"  make upgrade-pip     # upgrade pip/setuptools/wheel" \
	"  make initdb          # create/ensure DB schema" \
	"  make collect         # collect RSS/feeds" \
	"  make enrich          # language + topic tagging" \
	"  make cluster         # cluster items" \
	"  make summarize       # LLM summarization" \
	"  make digest          # build HTML digest" \
	"  make all             # run full pipeline" \
	"  make clean           # remove venv and build artifacts"

# -------- Environment setup --------
venv:
	@if [ ! -d "$(VENV)" ]; then \
		python -m venv "$(VENV)"; \
	fi

upgrade-pip: venv
	$(PIP) install --upgrade pip setuptools wheel

install-light: venv upgrade-pip
	$(PIP) install -r $(REQ_LIGHT)

install-full: venv upgrade-pip
	$(PIP) install -r $(REQ_FULL)

# -------- Pipeline steps --------
initdb: venv
	$(PY) scripts/initdb.py

collect: venv
	$(PY) -m src.collect_rss

enrich: venv
	$(PY) -m src.enrich

cluster: venv
	$(PY) -m src.cluster

summarize: venv
	$(PY) -m src.summarize

digest: venv
	$(PY) -m src.build_digest

all: collect enrich cluster summarize digest

# -------- Cleanup --------
clean:
	rm -rf $(VENV) build dist .pytest_cache .mypy_cache *.egg-info

