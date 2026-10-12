"""Сводка и графики бенчмарка → results/benchmark_summary.csv/.md и img/*.png.

Запуск: .venv/bin/python scripts/charts.py   (нужен matplotlib)
"""
import csv
import os
import statistics
from collections import defaultdict

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

ROOT = os.path.join(os.path.dirname(__file__), "..")
RES, IMG = os.path.join(ROOT, "results"), os.path.join(ROOT, "img")
os.makedirs(IMG, exist_ok=True)
FORMATS = ["PARQUET", "ORC", "AVRO"]
CODECS = ["NONE", "SNAPPY", "LZ4", "ZSTD", "GZIP"]
QUERIES = ["q10", "scan_agg", "narrow_sum"]

# ---------- запись и размер ----------
size, write, baseline, errors = defaultdict(float), defaultdict(float), 0.0, {}
for r in csv.DictReader(open(os.path.join(RES, "benchmark_write.csv"))):
    if r["format"] == "BLACKHOLE":
        baseline += float(r["write_ms_min"])
    elif r["error"]:
        errors[(r["format"], r["codec"])] = r["error"]
    else:
        k = (r["format"], r["codec"])
        size[k] += int(r["bytes"])
        write[k] += float(r["write_ms_min"])

# ---------- запросы: медиана по прогонам ----------
runs = defaultdict(list)
cpu = defaultdict(list)
inp = {}
for r in csv.DictReader(open(os.path.join(RES, "benchmark_query.csv"))):
    k = (r["format"], r["codec"], r["query"])
    runs[k].append(int(r["elapsed_ms"]))
    cpu[k].append(int(r["cpu_ms"]))
    inp[k] = int(r["physical_input_bytes"])
med = {k: statistics.median(v) for k, v in runs.items()}

configs = [(f, c) for f in FORMATS for c in CODECS if (f, c) in size and (f, c, "narrow_sum") in med]
base_size = size.get(("PARQUET", "NONE"))

with open(os.path.join(RES, "benchmark_summary.csv"), "w", newline="") as fcsv:
    w = csv.writer(fcsv)
    hdr = ["format", "codec", "bronze_mb", "ratio_vs_parquet_none", "write_s", "write_net_s"]
    for q in QUERIES:
        hdr += [f"{q}_median_s", f"{q}_cpu_s", f"{q}_read_mb"]
    w.writerow(hdr)
    md = ["| Формат | Кодек | Bronze, МБ | Сжатие vs PARQUET/NONE | Запись, с (чистая*) | Q10, с | scan_agg, с | narrow_sum, с | Прочитано Q10, МБ |",
          "|---|---|---:|---:|---:|---:|---:|---:|---:|"]
    for f, c in configs:
        k = (f, c)
        row = [f, c, round(size[k] / 1e6, 1), round(base_size / size[k], 2) if base_size else "",
               round(write[k] / 1000, 1), round((write[k] - baseline) / 1000, 1)]
        for q in QUERIES:
            row += [round(med[(f, c, q)] / 1000, 2), round(statistics.median(cpu[(f, c, q)]) / 1000, 2),
                    round(inp[(f, c, q)] / 1e6, 1)]
        w.writerow(row)
        md.append(f"| {f} | {c} | {row[2]:,.0f} | ×{row[3]} | {row[4]} ({row[5]}) | {row[6]} | {row[9]} | {row[12]} | {row[8]:,.0f} |"
                  .replace(",", " "))
    for (f, c), e in errors.items():
        md.append(f"| {f} | {c} | — | — | не поддерживается: `{e.split(':', 1)[1].strip()}` | | | | |")
open(os.path.join(RES, "benchmark_summary.md"), "w").write("\n".join(md) + "\n")
print("\n".join(md))
print(f"\nbaseline генерации tpch (blackhole), с: {baseline / 1000:.1f}")

# ---------- графики ----------
COLORS = {"PARQUET": "#2a6fdb", "ORC": "#e07b00", "AVRO": "#7a7a7a"}
labels = [f"{f}\n{c}" for f, c in configs]
x = range(len(configs))


def bar(values, title, ylabel, fname, fmt="{:.0f}"):
    fig, ax = plt.subplots(figsize=(12, 4.2))
    bars = ax.bar(x, values, color=[COLORS[f] for f, _ in configs])
    for b, v in zip(bars, values):
        ax.text(b.get_x() + b.get_width() / 2, b.get_height(), fmt.format(v), ha="center", va="bottom", fontsize=8)
    ax.set_xticks(list(x), labels, fontsize=8)
    ax.set_title(title)
    ax.set_ylabel(ylabel)
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()
    fig.savefig(os.path.join(IMG, fname), dpi=130)
    plt.close(fig)


bar([size[k] / 1e6 for k in configs], "Размер Bronze (TPC-H sf10, 5 таблиц), МБ", "МБ", "bench_size.png")
bar([(write[k] - baseline) / 1000 for k in configs],
    "Чистое время записи Bronze, с (CTAS минус генерация tpch → blackhole)", "секунды", "bench_write.png", "{:.1f}")

fig, axes = plt.subplots(1, 3, figsize=(15, 4.2), sharey=False)
for ax, q, title in zip(axes, QUERIES, ["Q10 (4 джойна + агрегат)", "scan_agg (скан lineitem, 7 колонок)",
                                         "narrow_sum (1 колонка lineitem)"]):
    vals = [med[(f, c, q)] / 1000 for f, c in configs]
    ax.bar(x, vals, color=[COLORS[f] for f, _ in configs])
    ax.set_xticks(list(x), [f"{f[:3]}\n{c}" for f, c in configs], fontsize=7)
    ax.set_title(title, fontsize=10)
    ax.set_ylabel("медиана, с")
    ax.spines[["top", "right"]].set_visible(False)
fig.suptitle("Скорость агрегаций по Bronze (медиана: 5 прогонов для PARQUET и ORC/NONE, 3 — для остальных; после прогрева)")
fig.tight_layout()
fig.savefig(os.path.join(IMG, "bench_queries.png"), dpi=130)
plt.close(fig)

# компромисс размер ↔ скорость Q10
fig, ax = plt.subplots(figsize=(7, 5))
for f, c in configs:
    ax.scatter(size[(f, c)] / 1e6, med[(f, c, "q10")] / 1000, color=COLORS[f], s=50)
    ax.annotate(f"{f[:3]}/{c}", (size[(f, c)] / 1e6, med[(f, c, "q10")] / 1000), fontsize=7,
                xytext=(4, 3), textcoords="offset points")
ax.set_xlabel("Размер Bronze, МБ (меньше — лучше)")
ax.set_ylabel("Q10, медиана, с (меньше — лучше)")
ax.set_title("Компромисс: размер vs скорость Q10")
ax.spines[["top", "right"]].set_visible(False)
fig.tight_layout()
fig.savefig(os.path.join(IMG, "bench_tradeoff.png"), dpi=130)
plt.close(fig)
print("графики сохранены в img/")
