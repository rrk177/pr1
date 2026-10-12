"""Бенчмарк форматов и кодеков на Bronze-слое (TPC-H sf10, вариант Q10).

Для каждой комбинации формат × кодек:
  1. создаётся схема lakekeeper.bench_<format>_<codec>;
  2. Bronze-таблицы (как в sql/01_bronze.sql) пишутся WRITE_RUNS раз через CTAS из tpch.sf10,
     фиксируется elapsedTime записи каждой таблицы;
  3. размер берётся из метаданных Iceberg ("<table>$files": сумма file_size_in_bytes);
  4. три аналитических запроса: 1 прогревочный + QUERY_RUNS замеров (медиана);
  5. таблицы удаляются (delete-profile = hard → файлы физически стираются из MinIO).

Baseline: та же генерация, но запись в каталог blackhole (данные выбрасываются) —
это стоимость генерации tpch без записи; её вычитаем, чтобы получить «чистое» время записи.

Запуск:  python3 scripts/benchmark.py            (все комбинации, ~1 час)
         python3 scripts/benchmark.py PARQUET:ZSTD ORC:ZSTD   (выборочно)
"""
import csv
import os
import statistics
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))
from trino import query  # noqa: E402

SF = os.environ.get("SF", "sf10")
FORMATS = ["PARQUET", "ORC", "AVRO"]
CODECS = ["NONE", "SNAPPY", "LZ4", "ZSTD", "GZIP"]
TABLES = ["nation", "region", "customer", "orders", "lineitem"]
WRITE_RUNS = int(os.environ.get("WRITE_RUNS", 2))
QUERY_RUNS = int(os.environ.get("QUERY_RUNS", 5))
OUT = os.environ.get("OUT", os.path.join(os.path.dirname(__file__), "..", "results"))


def bronze_select(table):
    # то же определение, что и в sql/01_bronze.sql: все колонки «как есть» + техколонки происхождения
    return (f"SELECT *, CAST(current_timestamp AS timestamp(6) with time zone) AS _ingested_at, "
            f"'tpch.{SF}.{table}' AS _source FROM tpch.{SF}.{table}")


QUERIES = {
    # 1) сам запрос варианта — Q10 Returned Item Reporting (4 таблицы, джойны + агрегат + top-20)
    "q10": """
        SELECT c.custkey, c.name, sum(l.extendedprice * (1 - l.discount)) AS revenue,
               c.acctbal, n.name AS nation, c.address, c.phone, c.comment
        FROM {s}.customer c
        JOIN {s}.orders o   ON c.custkey = o.custkey
        JOIN {s}.lineitem l ON l.orderkey = o.orderkey
        JOIN {s}.nation n   ON c.nationkey = n.nationkey
        WHERE o.orderdate >= DATE '1993-10-01' AND o.orderdate < DATE '1994-01-01'
          AND l.returnflag = 'R'
        GROUP BY c.custkey, c.name, c.acctbal, n.name, c.address, c.phone, c.comment
        ORDER BY revenue DESC LIMIT 20""",
    # 2) тяжёлый скан-агрегат по всему lineitem (7 колонок из 18) — стиль TPC-H Q1
    "scan_agg": """
        SELECT returnflag, linestatus, sum(quantity), sum(extendedprice),
               sum(extendedprice * (1 - discount)), avg(discount), count(*)
        FROM {s}.lineitem WHERE shipdate <= DATE '1998-09-02'
        GROUP BY returnflag, linestatus""",
    # 3) узкий агрегат по одной колонке — проверка «колоночности» формата
    "narrow_sum": "SELECT sum(extendedprice) FROM {s}.lineitem",
}


def table_size(schema, table):
    _, rows, _ = query(f'SELECT sum(file_size_in_bytes), count(*), sum(record_count) '
                       f'FROM {schema}."{table}$files"')
    return rows[0]


def append_csv(name, row, header):
    path = os.path.join(OUT, name)
    new = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        w = csv.writer(f)
        if new:
            w.writerow(header)
        w.writerow(row)


def baseline():
    """Стоимость генерации tpch без записи: CTAS → blackhole."""
    query("CREATE SCHEMA IF NOT EXISTS blackhole.bench")
    for t in TABLES:
        times = []
        for _ in range(WRITE_RUNS):
            query(f"DROP TABLE IF EXISTS blackhole.bench.{t}")
            _, _, st = query(f"CREATE TABLE blackhole.bench.{t} AS {bronze_select(t)}")
            times.append(st["elapsedTimeMillis"])
        print(f"  baseline {t}: {times} ms")
        append_csv("benchmark_write.csv", ["BLACKHOLE", "-", t, min(times), statistics.mean(times), "", "", "", ""],
                   WRITE_HDR)


WRITE_HDR = ["format", "codec", "table", "write_ms_min", "write_ms_mean", "bytes", "files", "rows", "error"]
QUERY_HDR = ["format", "codec", "query", "run", "elapsed_ms", "cpu_ms", "physical_input_bytes",
             "peak_memory_bytes", "query_id"]


def run_config(fmt, codec):
    schema = f"lakekeeper.bench_{fmt.lower()}_{codec.lower()}"
    session = {"lakekeeper.compression_codec": codec}
    query(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    try:
        for t in TABLES:
            times = []
            for _ in range(WRITE_RUNS):
                query(f"DROP TABLE IF EXISTS {schema}.{t}")
                _, _, st = query(f"CREATE TABLE {schema}.{t} WITH (format = '{fmt}') AS {bronze_select(t)}", session)
                times.append(st["elapsedTimeMillis"])
            size, files, rows = table_size(schema, t)
            print(f"  {fmt}/{codec} {t}: write {times} ms, {size / 1e6:.1f} MB in {files} files")
            append_csv("benchmark_write.csv", [fmt, codec, t, min(times), statistics.mean(times), size, files, rows, ""],
                       WRITE_HDR)
        for qname, sql in QUERIES.items():
            sql = sql.format(s=schema)
            query(sql)  # прогрев: JIT, метаданные, page cache MinIO
            el = []
            for i in range(QUERY_RUNS):
                _, _, st = query(sql)
                el.append(st["elapsedTimeMillis"])
                append_csv("benchmark_query.csv", [fmt, codec, qname, i + 1, st["elapsedTimeMillis"],
                                                   st["cpuTimeMillis"], st["physicalInputBytes"],
                                                   st["peakMemoryBytes"], st["queryId"]], QUERY_HDR)
            print(f"  {fmt}/{codec} {qname}: median {statistics.median(el)} ms  {el}")
    except RuntimeError as e:
        msg = str(e).splitlines()[0]
        print(f"  {fmt}/{codec}: ERROR {msg}")
        append_csv("benchmark_write.csv", [fmt, codec, "*", "", "", "", "", "", msg], WRITE_HDR)
    finally:
        for t in TABLES:
            query(f"DROP TABLE IF EXISTS {schema}.{t}")
        query(f"DROP SCHEMA IF EXISTS {schema}")


if __name__ == "__main__":
    os.makedirs(OUT, exist_ok=True)
    t0 = time.time()
    if len(sys.argv) > 1:
        configs = [tuple(a.split(":")) for a in sys.argv[1:]]
    else:
        configs = [(f, c) for f in FORMATS for c in CODECS]
        print("baseline (blackhole):")
        baseline()
    for fmt, codec in configs:
        print(f"== {fmt} / {codec}  (+{(time.time() - t0) / 60:.1f} min)", flush=True)
        run_config(fmt, codec)
    print(f"done in {(time.time() - t0) / 60:.1f} min")
