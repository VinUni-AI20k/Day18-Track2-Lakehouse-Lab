## Day 18 Lakehouse Lab — student UX
## Two paths: lightweight (default, pure Python) and Spark (Docker, optional).

VENV       := .venv
# On Windows use the Scripts\ path and .exe suffix; on Unix use bin/
ifeq ($(OS),Windows_NT)
PY         := $(VENV)\Scripts\python.exe
PIP        := $(VENV)\Scripts\pip.exe
JUPYTER    := $(VENV)\Scripts\jupyter.exe
JUPYTEXT   := $(VENV)\Scripts\jupytext.exe
DEVNULL    := NUL
CLEAN      := powershell -NoProfile -Command "Remove-Item -Recurse -Force '$(VENV)','_lakehouse','notebooks/.ipynb_checkpoints' -ErrorAction SilentlyContinue"
else
PY         := $(VENV)/bin/python
PIP        := $(VENV)/bin/pip
JUPYTER    := $(VENV)/bin/jupyter
JUPYTEXT   := $(VENV)/bin/jupytext
DEVNULL    := /dev/null
CLEAN      := rm -rf
endif
COMPOSE    := docker compose -f docker/docker-compose.yml

.DEFAULT_GOAL := help

help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n\nLightweight path (default — no Docker):\n"} \
	      /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

# ─────────────────────────────────────────────────────────────
# Lightweight path (default) — pure Python, no Docker, no JVM
# ─────────────────────────────────────────────────────────────

setup: ## [lite] Create venv + install deps (~80 MB, ~10s with pip / ~2s with uv)
	@command -v uv >/dev/null 2>&1 && uv venv $(VENV) || python -m venv $(VENV)
	@command -v uv >/dev/null 2>&1 && uv pip install --python $(PY) -r requirements.txt \
	  || $(PIP) install -q -r requirements.txt
	-@$(PY) -m jupytext --to notebook --update notebooks/*.py 2>$(DEVNULL)
	@echo ""
	@echo "  ✓ Setup complete. Run 'make smoke' then 'make lab'."

smoke: ## [lite] 5-second end-to-end smoke test
	@$(PY) scripts/verify_lite.py

lab: ## [lite] Open Jupyter Lab on http://localhost:8888
	-@$(PY) -m jupytext --to notebook --update notebooks/*.py 2>$(DEVNULL)
	@$(PY) -m jupyter lab --notebook-dir=notebooks --ServerApp.token='' --no-browser

data: ## [lite] Generate 200K-row Bronze sample for NB4
	@$(PY) scripts/generate_data_lite.py

clean: ## [lite] Wipe venv + lakehouse data
	@$(CLEAN) $(VENV) _lakehouse notebooks/.ipynb_checkpoints

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

.PHONY: help setup smoke lab data clean spark-up spark-smoke spark-data spark-down spark-clean
