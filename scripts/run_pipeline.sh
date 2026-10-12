#!/usr/bin/env bash
# Полный цикл медальона: Bronze → Silver → Gold → проверки. Время каждого шага печатается.
set -euo pipefail
cd "$(dirname "$0")/.."
for f in sql/01_bronze.sql sql/02_silver.sql sql/03_gold.sql sql/04_validate.sql; do
  echo "===== $f"
  python3 scripts/trino.py -f "$f"
done
