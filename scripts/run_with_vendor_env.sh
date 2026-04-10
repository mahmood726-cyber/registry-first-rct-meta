#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PYTHONPATH="$ROOT_DIR/.vendor/usr/lib/python3/dist-packages${PYTHONPATH:+:$PYTHONPATH}"
export LD_LIBRARY_PATH="$ROOT_DIR/.vendor/usr/lib/x86_64-linux-gnu:$ROOT_DIR/.vendor/usr/lib/x86_64-linux-gnu/blas:$ROOT_DIR/.vendor/usr/lib/x86_64-linux-gnu/lapack${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

cd "$ROOT_DIR"
python3 -m scripts.run_validation --data_dir data/cochrane_501 --out_dir outputs --max_reviews 501 --after_year 2010 --main_outcome_only true
python3 -m scripts.run_topic_engine --config data/topics/hfpef_sglt2.yaml --out_dir outputs
