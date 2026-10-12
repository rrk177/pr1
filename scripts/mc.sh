#!/usr/bin/env bash
# Обёртка над MinIO-клиентом (mc) в одноразовом контейнере: ./scripts/mc.sh du m/warehouse
docker run --rm --entrypoint /bin/sh pgsty/mc:latest -c \
  "mc alias set m http://host.docker.internal:9000 minioadmin minioadmin >/dev/null && mc $*"
