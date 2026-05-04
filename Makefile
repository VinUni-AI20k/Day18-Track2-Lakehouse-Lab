## Day 18 Lakehouse Lab — student UX
## Two paths: lightweight (default, pure Python) and Spark (Docker, optional).

VENV       := .venv
PY         := $(VENV)/Scripts/python
PIP        := $(VENV)/Scripts/pip
JUPYTER    := $(VENV)/Scripts/jupyter
JUPYTEXT   := $(VENV)/Scripts/jupytext
COMPOSE    := docker compose -f docker/docker-compose.yml

.DEFAULT_GOAL := help

help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n\nLightweight path (default — no Docker):\n"} \
	      /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

# ─────────────────────────────────────────────────────────────
# Lightweight path (default) — pure Python, no Docker, no JVM
# ─────────────────────────────────────────────────────────────

setup: ## [lite] Create venv + install deps (~80 MB, ~10s with pip / ~2s with uv)
	python -m venv $(VENV)
	$(PY) -m pip install -q -r requirements.txt
	$(PY) -m jupytext --to notebook --update notebooks/*.py || $(PY) -m jupytext --to notebook notebooks/*.py
	@echo ""
	@echo "  ✓ Setup complete. Run 'make smoke' then 'make lab'."

smoke: ## [lite] 5-second end-to-end smoke test
	@$(PY) scripts/verify_lite.py

lab: ## [lite] Open Jupyter Lab on http://localhost:8888
	$(PY) -m jupytext --to notebook --update notebooks/*.py
	$(PY) -m jupyter lab --notebook-dir=notebooks --ServerApp.token='' --no-browser

data: ## [lite] Generate 200K-row Bronze sample for NB4
	@$(PY) scripts/generate_data_lite.py

clean: ## [lite] Wipe venv + lakehouse data
	if exist $(VENV) rmdir /s /q $(VENV)
	if exist _lakehouse rmdir /s /q _lakehouse
	if exist notebooks\.ipynb_checkpoints rmdir /s /q notebooks\.ipynb_checkpoints

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
