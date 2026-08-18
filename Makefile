## Day 18 Lakehouse Lab — student UX (Windows Version)
## Two paths: lightweight (default, pure Python) and Spark (Docker, optional).

VENV       := .venv
PY         := $(VENV)/Scripts/python.exe
PIP        := $(VENV)/Scripts/pip.exe
JUPYTER    := $(VENV)/Scripts/jupyter.exe
JUPYTEXT   := $(VENV)/Scripts/jupytext.exe
PYTEST     := $(VENV)/Scripts/pytest.exe
COMPOSE    := docker compose -f docker/docker-compose.yml

.DEFAULT_GOAL := help

help: ## Show this help
	@echo "Usage: make [target]"
	@echo "------------------------------------------------------------"
	@echo "setup      : Create venv and install dependencies"
	@echo "smoke      : Run lightweight end-to-end smoke test"
	@echo "test       : Run pytest suite"
	@echo "lab        : Open Jupyter Lab on localhost:8888"
	@echo "data       : Generate sample data for NB4"
	@echo "data-ai    : Generate AI sample data for NB7/NB8"
	@echo "clean      : Remove venv and cache files"

# ─────────────────────────────────────────────────────────────
# Lightweight path (default) — pure Python, no Docker, no JVM
# ─────────────────────────────────────────────────────────────

setup: ## [lite] Create venv + install deps
	uv venv $(VENV) --python 3.12 || python -m venv $(VENV)
	uv pip install --python $(PY) -r requirements.txt || $(PIP) install -r requirements.txt
	$(JUPYTEXT) --to notebook --update notebooks/*.py || $(JUPYTEXT) --to notebook notebooks/*.py || exit 0
	@echo "Setup complete. Run 'make smoke' then 'make lab'."

smoke: ## [lite] ~15-second end-to-end smoke test (Delta + Iceberg + vectors)
	$(PY) scripts/verify_lite.py

test: ## [lite] Run the pytest suite the instructor grades against
	$(PYTEST) -q

lab: ## [lite] Open Jupyter Lab on http://localhost:8888
	$(JUPYTEXT) --to notebook --update notebooks/*.py || exit 0
	$(JUPYTER) lab --notebook-dir=notebooks --ServerApp.token='' --no-browser

data: ## [lite] Generate 200K-row Bronze sample for NB4
	$(PY) scripts/generate_data_lite.py

data-ai: ## [lite] Generate multimodal + agent-trajectory sample for NB7/NB8
	$(PY) scripts/generate_ai_data.py

run-all: ## [lite] Execute every notebook headlessly (what CI does)
	$(PY) scripts/run_all.py

simulate: ## [lite] Abuse the lab the way students do
	$(PY) tests/simulate_students.py

clean: ## [lite] Wipe venv + lakehouse data
	@python -c "import shutil, os; [shutil.rmtree(p, ignore_errors=True) for p in ['.venv', '_lakehouse', 'notebooks/.ipynb_checkpoints', '.pytest_cache']]"
	@echo "Clean completed."

# ─────────────────────────────────────────────────────────────
# Spark + Docker path (optional, production-fidelity)
# ─────────────────────────────────────────────────────────────

spark-up: ## [spark] Start MinIO + Spark/Jupyter
	$(COMPOSE) up -d
	@echo "Jupyter -> http://localhost:8888 (token: lakehouse)"
	@echo "MinIO   -> http://localhost:9001 (minioadmin / minioadmin)"

spark-smoke: ## [spark] Smoke test inside Spark container
	$(COMPOSE) exec -T spark python /workspace/scripts/verify.py

spark-data: ## [spark] Generate 1M-row Bronze (Spark version)
	$(COMPOSE) exec -T spark python /workspace/scripts/generate_data.py

spark-down: ## [spark] Stop Docker stack (data persists)
	$(COMPOSE) down

spark-clean: ## [spark] Stop AND wipe MinIO + ivy cache
	$(COMPOSE) down -v

.PHONY: help setup smoke test lab data data-ai run-all clean \
        simulate spark-up spark-smoke spark-data spark-down spark-clean