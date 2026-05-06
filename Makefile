## Day 18 Lakehouse Lab — student UX
## Two paths: lightweight (default, pure Python) and Spark (Docker, optional).

VENV       := .venv
ifeq ($(OS),Windows_NT)
PY         := $(VENV)/Scripts/python
PIP        := $(VENV)/Scripts/pip
JUPYTER    := $(VENV)/Scripts/jupyter
JUPYTEXT   := $(VENV)/Scripts/jupytext
PYTHON_CMD := python
else
PY         := $(VENV)/bin/python
PIP        := $(VENV)/bin/pip
JUPYTER    := $(VENV)/bin/jupyter
JUPYTEXT   := $(VENV)/bin/jupytext
PYTHON_CMD := python3
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
	command -v uv >/dev/null 2>&1 && uv venv $(VENV) || $(PYTHON_CMD) -m venv $(VENV)
	command -v uv >/dev/null 2>&1 && uv pip install --python $(PY) -r requirements.txt \
	  || $(PIP) install -q -r requirements.txt
	$(JUPYTEXT) --to notebook --update notebooks/*.py 2>NUL || $(JUPYTEXT) --to notebook notebooks/*.py
	@echo ""
	@echo "  ✓ Setup complete. Run 'make smoke' then 'make lab'."

smoke: ## [lite] 5-second end-to-end smoke test
	@$(PY) scripts/verify_lite.py

lab: ## [lite] Open Jupyter Lab on http://localhost:8888
	@$(JUPYTEXT) --to notebook --update notebooks/*.py 2>/dev/null || true
	@$(JUPYTER) lab --notebook-dir=notebooks --ServerApp.token='' --no-browser

data: ## [lite] Generate 200K-row Bronze sample for NB4
	@$(PY) scripts/generate_data_lite.py

clean: ## [lite] Wipe venv + lakehouse data
ifeq ($(OS),Windows_NT)
	python -c "import shutil; shutil.rmtree('$(VENV)', ignore_errors=True); shutil.rmtree('_lakehouse', ignore_errors=True); shutil.rmtree('notebooks/.ipynb_checkpoints', ignore_errors=True)"
else
	rm -rf $(VENV) _lakehouse notebooks/.ipynb_checkpoints
endif

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
