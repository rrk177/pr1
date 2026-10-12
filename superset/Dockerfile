# Официальный образ Superset + драйвер Trino (sqlalchemy-диалект trino://)
FROM apache/superset:4.1.2
USER root
RUN pip install --no-cache-dir "trino[sqlalchemy]>=0.328,<0.400"
USER superset
