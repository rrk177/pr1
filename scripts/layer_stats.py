"""Статистика по слоям медальона: строки, размер, файлы, формат/кодек, партиции.

Размер и число строк берутся из метаданных Iceberg ("<t>$files"), формат и кодек —
из самих data-файлов (поле file_format) и свойств таблицы.
Результат: results/layer_stats.csv + печать markdown-таблицы.
"""
import csv
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from trino import query  # noqa: E402

OUT = os.path.join(os.path.dirname(__file__), "..", "results", "layer_stats.csv")
rows_out = []
for schema in ["bronze", "silver", "gold"]:
    _, tables, _ = query(f"SHOW TABLES FROM lakekeeper.{schema}")
    for (t,) in tables:
        # content = 0 — только data-файлы (без delete-файлов от UPDATE/DELETE); строки — честный count(*)
        _, r, _ = query(f'SELECT sum(file_size_in_bytes), count(*), arbitrary(file_format) '
                        f'FROM lakekeeper.{schema}."{t}$files" WHERE content = 0')
        size, files, fmt = r[0]
        rows = query(f"SELECT count(*) FROM lakekeeper.{schema}.{t}")[1][0][0]
        _, ddl, _ = query(f"SHOW CREATE TABLE lakekeeper.{schema}.{t}")
        ddl = ddl[0][0]
        part = "нет"
        if "partitioning" in ddl:
            part = ddl.split("partitioning = ARRAY[")[1].split("]")[0].replace("'", "")
        _, cols, _ = query(f"SELECT count(*) FROM lakekeeper.information_schema.columns "
                           f"WHERE table_schema = '{schema}' AND table_name = '{t}'")
        rows_out.append([schema, t, rows, round(size / 1e6, 2), files, cols[0][0], fmt, part])

with open(OUT, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["layer", "table", "rows", "size_mb", "files", "columns", "format", "partitioning"])
    w.writerows(rows_out)

print("| Слой | Таблица | Строк | Вес, МБ | Файлов | Колонок | Формат | Партиции |")
print("|---|---|---:|---:|---:|---:|---|---|")
for r in rows_out:
    print(f"| {r[0]} | {r[1]} | {r[2]:,} | {r[3]:,} | {r[4]} | {r[5]} | {r[6]} | {r[7]} |".replace(",", " "))
