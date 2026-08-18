## Day 18 Lakehouse Lab — student UX
## Two paths: lightweight (default, pure Python) and Spark (Docker, optional).

VENV       := .venv

ifeq ($(OS),Windows_NT)
  BIN      := .venv\Scripts
  PY       := .venv\Scripts\python.exe
  PIP      := .venv\Scripts\pip.exe
  JUPYTER  := .venv\Scripts\jupyter.exe
  JUPYTEXT := .venv\Scripts\jupytext.exe
  PYTEST   := .venv\Scripts\pytest.exe
  PY_CMD   := python
  DEVNULL  := NUL
else
  BIN      := .venv/bin
  PY       := .venv/bin/python
  PIP      := .venv/bin/pip
  JUPYTER  := .venv/bin/jupyter
  JUPYTEXT := .venv/bin/jupytext
  PYTEST   := .venv/bin/pytest
  PY_CMD   := python3
  DEVNULL  := /dev/null
endif
COMPOSE  := docker compose -f docker/docker-compose.yml

.DEFAULT_GOAL := help

help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n\nLightweight path (default — no Docker):\n"} \
	      /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

# ─────────────────────────────────────────────────────────────
# Lightweight path (default) — pure Python, no Docker, no JVM
# ─────────────────────────────────────────────────────────────

setup: ## [lite] Create venv + install deps (~180 MB, ~20s with pip / ~4s with uv)
	@uv --version > $(DEVNULL) 2>&1 && uv venv --allow-existing $(VENV) --python ">=3.10,<3.15" || $(PY_CMD) -m venv $(VENV)
	@$(PY) -c "import sys; (3,10)<=sys.version_info[:2]<(3,15) or (print('ERROR: need Python 3.10-3.14') or sys.exit(1))"
	@uv --version > $(DEVNULL) 2>&1 && uv pip install --python $(PY) -r requirements.txt \
	  || $(PIP) install -q -r requirements.txt
	@$(PY) -m jupytext --to notebook --update notebooks/*.py 2>$(DEVNULL) || $(PY) -m jupytext --to notebook notebooks/*.py 2>$(DEVNULL) || true
	@$(PY) -c "print('\n  ✓ Setup complete. Run \'make smoke\' then \'make lab\'.')"

smoke: ## [lite] ~15-second end-to-end smoke test (Delta + Iceberg + vectors)
	@$(PY) scripts/verify_lite.py

test: ## [lite] Run the pytest suite the instructor grades against
	@$(PYTEST) -q

lab: ## [lite] Open Jupyter Lab on http://localhost:8888
	@$(PY) -m jupytext --to notebook --update notebooks/*.py 2>$(DEVNULL) || true
	@$(JUPYTER) lab --notebook-dir=notebooks --ServerApp.token='' --no-browser

data: ## [lite] Generate 200K-row Bronze sample for NB4
	@$(PY) scripts/generate_data_lite.py

data-ai: ## [lite] Generate multimodal + agent-trajectory sample for NB7/NB8
	@$(PY) scripts/generate_ai_data.py

run-all: ## [lite] Execute every notebook headlessly (what CI does)
	@$(PY) scripts/run_all.py

simulate: ## [lite] Abuse the lab the way students do (12 scenarios; SIM_FAST=1 to skip venv builds)
	@$(PY) tests/simulate_students.py

clean: ## [lite] Wipe venv + lakehouse data
	@$(PY_CMD) -c "import shutil, os; [shutil.rmtree(p, ignore_errors=True) for p in ['$(VENV)', '_lakehouse', 'notebooks/.ipynb_checkpoints', '.pytest_cache']]"

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
