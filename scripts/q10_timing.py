"""Время запроса Q10 на слоях медальона (1 прогрев + 5 замеров, медиана) → results/q10_timing.csv."""
import csv
import os
import re
import statistics
import sys

sys.path.insert(0, os.path.dirname(__file__))
from benchmark import QUERIES  # noqa: E402
from trino import query  # noqa: E402

ROOT = os.path.join(os.path.dirname(__file__), "..")
gold_sql = open(os.path.join(ROOT, "sql", "03_gold.sql"), encoding="utf-8").read()
silver_q10 = re.search(r"\(\n\s+(SELECT\n\s+c\.custkey.*?LIMIT 20)\n\);", gold_sql, re.S).group(1)

CASES = {
    "tpch.sf10 (генерация на лету)": QUERIES["q10"].format(s="tpch.sf10"),
    "Bronze (PARQUET+ZSTD, без партиций)": QUERIES["q10"].format(s="lakekeeper.bronze"),
    "Silver (order_lines по month(orderdate))": silver_q10,
    "Gold (готовая витрина)": "SELECT * FROM lakekeeper.gold.q10_returned_items_top20 ORDER BY rank",
}
with open(os.path.join(ROOT, "results", "q10_timing.csv"), "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["source", "median_ms", "cpu_ms", "read_mb", "runs_ms"])
    for name, sql in CASES.items():
        query(sql)
        st = [query(sql)[2] for _ in range(5)]
        el = [s["elapsedTimeMillis"] for s in st]
        row = [name, statistics.median(el), statistics.median(s["cpuTimeMillis"] for s in st),
               round(st[-1]["physicalInputBytes"] / 1e6, 1), el]
        w.writerow(row)
        print(row, flush=True)
