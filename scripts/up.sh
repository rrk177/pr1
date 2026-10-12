#!/usr/bin/env bash
# Подъём локального Lakehouse: Silo (S3, форк MinIO) → Postgres → Lakekeeper (Iceberg REST) → Trino.
# Запускать из корня репозитория: ./scripts/up.sh
set -euo pipefail
cd "$(dirname "$0")/.."

# 1. Объектное хранилище. Официальные образы MinIO больше не раздаются бесплатно,
#    поэтому свободный форк Silo — тот же S3 API, порты и консоль.
docker run -d --name lakehouse-minio \
  -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  -v minio-data:/data \
  pgsty/silo:latest server /data --console-address ":9001"

# бакет warehouse — сюда Iceberg кладёт data/ и metadata/ всех таблиц
sleep 3
docker run --rm --entrypoint /bin/sh pgsty/mc:latest -c \
  "mc alias set m http://host.docker.internal:9000 minioadmin minioadmin && mc mb --ignore-existing m/warehouse"

# 2. Postgres — хранилище метаданных каталога Lakekeeper.
#    Хостовый порт 5433, т.к. 5432 на машине занят локальным Postgres.
docker run -d --name lakehouse-pg \
  -e POSTGRES_PASSWORD=postgres \
  -p 5433:5432 \
  -v pg-data:/var/lib/postgresql/data \
  postgres:17
until docker exec lakehouse-pg pg_isready >/dev/null 2>&1; do sleep 1; done

# 3. Lakekeeper — Iceberg REST Catalog: миграции схемы БД, затем сервер на :8181
LK_ENV=(-e 'LAKEKEEPER__PG_ENCRYPTION_KEY=This-is-NOT-Secure!'
        -e 'LAKEKEEPER__PG_DATABASE_URL_READ=postgresql://postgres:postgres@host.docker.internal:5433/postgres'
        -e 'LAKEKEEPER__PG_DATABASE_URL_WRITE=postgresql://postgres:postgres@host.docker.internal:5433/postgres')
docker run --rm "${LK_ENV[@]}" quay.io/lakekeeper/catalog:latest-main migrate
docker run -d --name lakekeeper "${LK_ENV[@]}" -p 8181:8181 quay.io/lakekeeper/catalog:latest-main serve
until curl -sf http://localhost:8181/health >/dev/null; do sleep 1; done

# принять Terms of Use и зарегистрировать warehouse "demo" → бакет warehouse в MinIO
curl -s -X POST http://localhost:8181/management/v1/bootstrap \
  -H "Content-Type: application/json" -d '{"accept-terms-of-use": true}' || true
curl -s -X POST http://localhost:8181/management/v1/warehouse \
  -H "Content-Type: application/json" -d @create-warehouse.json || true

# 4. Trino — SQL-движок. Каталоги: lakekeeper (Iceberg), tpch (генератор), blackhole (baseline бенчмарка)
docker run -d --name trino \
  -p 8080:8080 \
  -v "$(pwd)/catalog:/etc/trino/catalog" \
  trinodb/trino:476
until [ "$(docker inspect -f '{{.State.Health.Status}}' trino)" = healthy ]; do sleep 2; done
docker ps --format 'table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}'
