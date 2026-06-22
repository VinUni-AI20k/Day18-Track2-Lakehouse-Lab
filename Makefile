## Day 18 Lakehouse Lab — student UX
## Two paths: lightweight (default, pure Python) and Spark (Docker, optional).

VENV       := .venv
PY         := $(VENV)/bin/python
PIP        := $(VENV)/bin/pip
JUPYTER    := $(VENV)/bin/jupyter
JUPYTEXT   := $(VENV)/bin/jupytext
COMPOSE    := docker compose -f docker/docker-compose.yml

.DEFAULT_GOAL := help

help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n\nLightweight path (default — no Docker):\n"} \
	      /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

# ─────────────────────────────────────────────────────────────
# Lightweight path (default) — pure Python, no Docker, no JVM
# ─────────────────────────────────────────────────────────────

setup: ## [lite] Create venv + install deps (~80 MB, ~10s with pip / ~2s with uv)
	@command -v uv >/dev/null 2>&1 && uv venv $(VENV) --python '>=3.10,<3.14' || python3 -m venv $(VENV)
	@$(PY) -c 'import sys; raise SystemExit(0 if (3,10)<=sys.version_info[:2]<(3,14) else 1)' \
	  || { echo "ERROR: need Python 3.10-3.13 (pyarrow has no 3.14 wheel yet). Install 'uv' (auto-fetches 3.12) or run: python3.12 -m venv .venv"; exit 1; }
	@command -v uv >/dev/null 2>&1 && uv pip install --python $(PY) -r requirements.txt \
	  || $(PIP) install -q -r requirements.txt
	@$(JUPYTEXT) --to notebook --update notebooks/*.py 2>/dev/null || $(JUPYTEXT) --to notebook notebooks/*.py
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
	rm -rf $(VENV) _lakehouse notebooks/.ipynb_checkpoints

# ─────────────────────────────────────────────────────────────
# Spark + Docker path (optional, production-fidelity)
# ─────────────────────────────────────────────────────────────

spark-up: ## [spark] Start MinIO + Spark/Jupyter (Docker — first run pulls ~2 GB)
	$(COMPOSE) up -d
	@echo "  Jupyter → http://localhost:8888 (token: lakehouse)"
	@echo "  MinIO   → http://localhost:9001 (minioadmin / minioadmin)"

spark-ivy-perms:
	$(COMPOSE) exec -T -u root spark bash -lc 'mkdir -p /home/jovyan/.ivy2/cache /home/jovyan/.ivy2/jars && chown -R 1000:100 /home/jovyan/.ivy2'

spark-ready: spark-ivy-perms
	@echo "  Waiting for Spark Python deps inside container…"
	@for i in $$(seq 1 120); do \
		$(COMPOSE) exec -T -u jovyan spark bash -lc 'source /usr/local/bin/before-notebook.d/10spark-config.sh && python -c "import pyspark, delta, jupytext"' >/dev/null 2>&1 && exit 0; \
		sleep 2; \
	done; \
	echo "ERROR: Spark container did not become ready in time"; exit 1

spark-smoke: spark-ready ## [spark] Smoke test inside Spark container
	$(COMPOSE) exec -T -u jovyan spark bash -lc 'source /usr/local/bin/before-notebook.d/10spark-config.sh && python /workspace/scripts/verify.py'

spark-data: spark-ready ## [spark] Generate 1M-row Bronze (Spark version)
	$(COMPOSE) exec -T -u jovyan spark bash -lc 'source /usr/local/bin/before-notebook.d/10spark-config.sh && python /workspace/scripts/generate_data.py'

spark-lab: spark-ready ## [spark] Execute all 4 Spark notebooks and save outputs
	$(COMPOSE) exec -T -u jovyan spark bash -lc 'source /usr/local/bin/before-notebook.d/10spark-config.sh && cd /workspace && python /workspace/scripts/generate_data.py && (python -m jupytext --to notebook --update notebooks-spark/*.py 2>/dev/null || python -m jupytext --to notebook notebooks-spark/*.py) && jupyter nbconvert --to notebook --execute --inplace --ExecutePreprocessor.timeout=1800 notebooks-spark/01_delta_basics.ipynb notebooks-spark/02_optimize_zorder.ipynb notebooks-spark/03_time_travel.ipynb notebooks-spark/04_medallion.ipynb'

spark-down: ## [spark] Stop Docker stack (data persists)
	$(COMPOSE) down

spark-clean: ## [spark] Stop AND wipe MinIO + ivy cache
	$(COMPOSE) down -v

.PHONY: help setup smoke lab data clean spark-up spark-ivy-perms spark-ready spark-smoke spark-data spark-lab spark-down spark-clean
