## Day 18 Lakehouse Lab — student UX
## Two paths: lightweight (default, pure Python) and Spark (Docker, optional).

VENV             := .venv
NOTEBOOK_SOURCES := $(filter-out notebooks/_setup.py,$(wildcard notebooks/*.py))
COMPOSE          := docker compose -f docker/docker-compose.yml

# Keep Jupyter's signatures, config and runtime files inside the project.
# This avoids readonly-profile errors on managed Windows machines, and the
# whole directory is already covered by the `_lakehouse/` gitignore rule.
export JUPYTER_DATA_DIR    := $(CURDIR)/_lakehouse/.jupyter/data
export JUPYTER_CONFIG_DIR  := $(CURDIR)/_lakehouse/.jupyter/config
export JUPYTER_RUNTIME_DIR := $(CURDIR)/_lakehouse/.jupyter/runtime

# Windows commonly starts Python with a legacy console code page (for example
# CP1258), while the lab prints Unicode checkmarks and arrows.
export PYTHONUTF8       := 1
export PYTHONIOENCODING := utf-8

# Virtual environments use different executable directories on Windows and
# POSIX. Windows cmd.exe also requires backslashes for relative executables;
# otherwise it parses `/Scripts/...` as command-line switches for `.venv`.
ifeq ($(OS),Windows_NT)
HOST_PYTHON ?= python
PY          := $(VENV)\Scripts\python.exe
JUPYTER     := $(VENV)\Scripts\jupyter.exe
UV_VERSION  := $(shell uv --version 2>NUL)
else
HOST_PYTHON ?= python3
PY          := $(VENV)/bin/python
JUPYTER     := $(VENV)/bin/jupyter
UV_VERSION  := $(shell uv --version 2>/dev/null)
endif

PIP      := $(PY) -m pip
JUPYTEXT := $(PY) -m jupytext
PYTEST   := $(PY) -m pytest

ifneq ($(strip $(UV_VERSION)),)
CREATE_VENV := uv venv $(VENV) --python $(HOST_PYTHON) --allow-existing
INSTALL_DEPS := uv pip install --python $(PY) -r requirements.txt
else
CREATE_VENV := $(HOST_PYTHON) -m venv $(VENV)
INSTALL_DEPS := $(PIP) install -q -r requirements.txt
endif

.DEFAULT_GOAL := help

help: ## Show this help
	@$(HOST_PYTHON) -c "print('Usage: make <target>\n\nLightweight (no Docker):\n  setup          Create venv and install dependencies\n  smoke          Run the offline smoke test\n  test           Run the pytest grading suite\n  data           Generate Bronze data for NB4\n  data-ai        Generate data for NB7/NB8\n  lab            Open Jupyter Lab\n  run-all        Execute all 8 notebooks headlessly\n  clean          Remove generated local state\n\nOptional: spark-up, spark-smoke, spark-data, spark-down, spark-clean')"

# ─────────────────────────────────────────────────────────────
# Lightweight path (default) — pure Python, no Docker, no JVM
# ─────────────────────────────────────────────────────────────

setup: ## [lite] Create venv + install deps (~180 MB, uv preferred)
	@$(HOST_PYTHON) -c "import sys; raise SystemExit(0 if (3,10) <= sys.version_info[:2] < (3,15) else 'ERROR: need Python 3.10-3.14')"
	@$(CREATE_VENV)
	@$(INSTALL_DEPS)
	@$(JUPYTEXT) --to notebook --update $(NOTEBOOK_SOURCES) || $(JUPYTEXT) --to notebook $(NOTEBOOK_SOURCES)
	@$(HOST_PYTHON) -c "print('Setup complete. Run make smoke, then make lab.')"

smoke: ## [lite] ~15-second end-to-end smoke test (Delta + Iceberg + vectors)
	@$(PY) scripts/verify_lite.py

test: ## [lite] Run the pytest suite the instructor grades against
	@$(PYTEST) -q

lab: ## [lite] Open Jupyter Lab on http://localhost:8888
	-@$(JUPYTEXT) --to notebook --update $(NOTEBOOK_SOURCES)
	@$(JUPYTER) lab --notebook-dir=notebooks --ServerApp.token= --no-browser

data: ## [lite] Generate 200K-row Bronze sample for NB4
	@$(PY) scripts/generate_data_lite.py

data-ai: ## [lite] Generate multimodal + agent-trajectory sample for NB7/NB8
	@$(PY) scripts/generate_ai_data.py

run-all: ## [lite] Execute every notebook headlessly (what CI does)
	@$(PY) scripts/run_all.py

simulate: ## [lite] Abuse the lab the way students do (12 scenarios; SIM_FAST=1 to skip venv builds)
	@$(PY) tests/simulate_students.py

clean: ## [lite] Wipe venv + lakehouse data
	@$(HOST_PYTHON) -c "from pathlib import Path; import shutil; [shutil.rmtree(Path(p), ignore_errors=True) for p in ('.venv', '_lakehouse', 'notebooks/.ipynb_checkpoints', '.pytest_cache')]"

# ─────────────────────────────────────────────────────────────
# Spark on Apple `container` (optional) — macOS 15+, Apple silicon
# Same 3-service stack as the compose path, driven by `container run`,
# because Apple's runtime has no compose plugin and no Docker socket.
# ─────────────────────────────────────────────────────────────

AC := scripts/apple_container.sh

apple-up: ## [apple] Start MinIO + buckets + Spark/Jupyter via Apple `container`
	@$(AC) up

apple-smoke: ## [apple] Run scripts/verify.py in the Spark container
	@$(AC) smoke

apple-data: ## [apple] Generate the 1M-row Bronze table via Spark
	@$(AC) data

apple-status: ## [apple] Show containers + MinIO's injected IP
	@$(AC) status

apple-down: ## [apple] Stop and remove containers (MinIO data kept)
	@$(AC) down

apple-clean: ## [apple] Same as apple-down, plus delete _minio-data/
	@$(AC) clean

# ─────────────────────────────────────────────────────────────
# Spark + Docker path (optional, production-fidelity)
# ─────────────────────────────────────────────────────────────

spark-up: ## [spark] Start MinIO + Spark/Jupyter (Docker — first run pulls ~2 GB)
	$(COMPOSE) up -d
	@echo "  Jupyter → http://localhost:8888 (token: lakehouse)"
	@echo "  MinIO   → http://localhost:9001 (minioadmin / minioadmin)"

spark-smoke: ## [spark] Smoke test inside Spark container
	$(COMPOSE) exec -T spark python /workspace/scripts/verify.py

spark-data: ## [spark] Generate 1M-row Bronze (Spark version)
	$(COMPOSE) exec -T spark python /workspace/scripts/generate_data.py

spark-down: ## [spark] Stop Docker stack (data persists)
	$(COMPOSE) down

spark-clean: ## [spark] Stop AND wipe MinIO + ivy cache
	$(COMPOSE) down -v

.PHONY: help setup smoke test lab data data-ai run-all clean \
        simulate apple-up apple-smoke apple-data apple-status apple-down apple-clean \
        spark-up spark-smoke spark-data spark-down spark-clean