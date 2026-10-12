#!/usr/bin/env bash
# Часть 2: BI-инструмент Apache Superset поверх lakehouse (Trino → Iceberg/Lakekeeper → S3).
set -euo pipefail
cd "$(dirname "$0")/.."

docker build -t lakehouse-superset:4.1.2 superset/   # официальный образ + драйвер trino

docker run -d --name superset \
  -p 8088:8088 \
  -v "$(pwd)/superset/superset_config.py:/app/pythonpath/superset_config.py:ro" \
  -v superset-home:/app/superset_home \
  lakehouse-superset:4.1.2

# инициализация: схема метабазы, админ, роли
docker exec superset superset db upgrade
docker exec superset superset fab create-admin --username admin --password admin \
  --firstname Admin --lastname Lakehouse --email admin@example.com || true
docker exec superset superset init

# подключение к lakehouse: Trino-каталог lakekeeper (Iceberg REST) — URI для Superset
docker exec superset superset set_database_uri \
  --database_name "Lakehouse (Trino → Iceberg)" \
  --uri "trino://trino@host.docker.internal:8080/lakekeeper"
echo "Superset: http://localhost:8088  (admin / admin)"
