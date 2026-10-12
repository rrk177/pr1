"""Минимальный клиент Trino REST API на стандартной библиотеке Python.

Использование из консоли:
    python3 scripts/trino.py "SELECT 1"
    python3 scripts/trino.py -f sql/01_bronze.sql      # выполнить файл (запросы разделены ';')
"""
import json
import sys
import time
import urllib.request

TRINO = "http://localhost:8080"
HEADERS = {"X-Trino-User": "trino", "X-Trino-Source": "lakehouse-q10"}


def _req(method, url, body=None):
    req = urllib.request.Request(url, data=body, method=method, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=3600) as r:
        return json.loads(r.read())


def query(sql, session=None):
    """Выполняет SQL, возвращает (columns, rows, stats).

    stats — финальная статистика Trino (elapsedTimeMillis, cpuTimeMillis,
    processedRows, processedBytes, ...) + 'wall_s' — время, замеренное клиентом.
    """
    headers = dict(HEADERS)
    if session:
        headers["X-Trino-Session"] = ",".join(f"{k}={v}" for k, v in session.items())
    t0 = time.perf_counter()
    req = urllib.request.Request(f"{TRINO}/v1/statement", data=sql.encode(), method="POST", headers=headers)
    with urllib.request.urlopen(req, timeout=3600) as r:
        resp = json.loads(r.read())
    columns, rows = None, []
    while True:
        if "error" in resp:
            err = resp["error"]
            raise RuntimeError(f"{err.get('errorName')}: {err.get('message')}\nSQL: {sql[:300]}")
        if resp.get("columns") and columns is None:
            columns = [c["name"] for c in resp["columns"]]
        rows.extend(resp.get("data", []))
        nxt = resp.get("nextUri")
        if not nxt:
            break
        resp = _req("GET", nxt)
    stats = resp.get("stats", {})
    stats["wall_s"] = time.perf_counter() - t0
    stats["queryId"] = resp.get("id")
    return columns, rows, stats


def split_sql(text):
    """Разбивает скрипт на запросы по ';', выкидывая строки-комментарии '--'."""
    lines = [l for l in text.splitlines() if not l.strip().startswith("--")]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


def run_file(path, verbose=True, session=None):
    """Выполняет SQL-файл. 'SET SESSION k = v' запоминается и шлётся заголовком
    X-Trino-Session со всеми последующими запросами (как это делает trino-cli)."""
    session = dict(session or {})
    for stmt in split_sql(open(path, encoding="utf-8").read()):
        if stmt.upper().startswith("SET SESSION"):
            k, v = stmt[len("SET SESSION"):].split("=", 1)
            session[k.strip()] = v.strip().strip("'")
            if verbose:
                print(f"[session] {k.strip()} = {session[k.strip()]}")
            continue
        cols, rows, st = query(stmt, session)
        if verbose:
            head = stmt.splitlines()[0][:90]
            print(f"[{st['wall_s']:7.2f}s] {head}")
            if cols and rows and len(rows) <= 30 and not stmt.upper().startswith(("CREATE", "INSERT", "DROP", "ALTER")):
                print_table(cols, rows)


def print_table(cols, rows):
    w = [max(len(str(c)), *(len(str(r[i])) for r in rows)) for i, c in enumerate(cols)]
    print(" | ".join(str(c).ljust(w[i]) for i, c in enumerate(cols)))
    print("-+-".join("-" * x for x in w))
    for r in rows:
        print(" | ".join(str(v).ljust(w[i]) for i, v in enumerate(r)))


if __name__ == "__main__":
    if sys.argv[1] == "-f":
        run_file(sys.argv[2])
    else:
        c, r, s = query(sys.argv[1])
        if c and r:
            print_table(c, r)
        print(f"-- {len(r)} rows, {s['wall_s']:.2f}s, queryId={s['queryId']}")
