"""Часть 2: schema evolution и time travel на silver.customer.

Шаги (весь вывод пишется в results/evolution_timetravel.log):
  1. снапшоты таблицы до изменений;
  2. schema evolution: ADD COLUMN customer_tier + заполнение (UPDATE), RENAME COLUMN phone → phone_number;
     проверяем, что старые снапшоты читаются (Iceberg адресует колонки по field-id, не по имени);
  3. «авария»: случайно удалили всех клиентов из RUSSIA;
  4. time travel: читаем таблицу FOR VERSION AS OF <снапшот до аварии>;
  5. восстановление: rollback_to_snapshot к снапшоту до аварии;
  6. возвращаем имя phone (чтобы Gold-скрипты продолжали работать).
"""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from trino import query  # noqa: E402

T = "lakekeeper.silver.customer"
LOG = open(os.path.join(os.path.dirname(__file__), "..", "results", "evolution_timetravel.log"), "w")


def run(sql, title=None):
    cols, rows, st = query(sql)
    lines = []
    if title:
        lines.append(f"\n### {title}")
    lines.append("```sql\n" + sql.strip() + "\n```")
    if cols and rows and not sql.lstrip().upper().startswith(("ALTER", "UPDATE", "DELETE", "INSERT")):
        w = [max(len(str(c)), *(len(str(r[i])) for r in rows)) for i, c in enumerate(cols)]
        lines.append("| " + " | ".join(str(c) for c in cols) + " |")
        lines.append("|" + "|".join("---" for _ in cols) + "|")
        lines += ["| " + " | ".join(str(v) for v in r) + " |" for r in rows[:15]]
    elif rows:
        lines.append(f"-- затронуто строк: {rows[0][0]}")
    text = "\n".join(lines)
    print(text)
    LOG.write(text + "\n")
    LOG.flush()
    return cols, rows


SNAP = 'lakekeeper.silver."customer$snapshots"'


def current_snapshot():
    return query(f"SELECT snapshot_id FROM {SNAP} ORDER BY committed_at DESC LIMIT 1")[1][0][0]

run(f"SELECT snapshot_id, committed_at, operation FROM {SNAP} ORDER BY committed_at", "1. Снапшоты до изменений")
s0 = current_snapshot()
run(f"SELECT count(*) AS rows, count(DISTINCT nation_name) AS nations FROM {T}", "Исходное состояние")

# ---- 2. schema evolution ----
run(f"ALTER TABLE {T} ADD COLUMN customer_tier varchar", "2a. Schema evolution: добавляем колонку")
run(f"SELECT custkey, name, acctbal, customer_tier FROM {T} ORDER BY custkey LIMIT 3",
    "Старые строки не переписывались — новая колонка у них NULL")
run(f"""UPDATE {T} SET customer_tier = CASE
            WHEN acctbal >= 5000 THEN 'GOLD'
            WHEN acctbal >= 0    THEN 'STANDARD'
            ELSE 'DEBTOR' END""", "2b. Заполняем новую колонку (обогащение)")
run(f"SELECT customer_tier, count(*) AS customers FROM {T} GROUP BY customer_tier ORDER BY 1")
run(f"ALTER TABLE {T} RENAME COLUMN phone TO phone_number", "2c. Schema evolution: переименовываем колонку")
run(f"SELECT custkey, phone_number, customer_tier FROM {T} ORDER BY custkey LIMIT 3")
run(f"SELECT custkey, name, acctbal FROM {T} FOR VERSION AS OF {s0} ORDER BY custkey LIMIT 3",
    "Снапшот ДО эволюции схемы по-прежнему читается")
run(f"SELECT snapshot_id, committed_at, operation, element_at(summary, 'added-records') AS added, "
    f"element_at(summary, 'deleted-records') AS deleted FROM {SNAP} ORDER BY committed_at", "Снапшоты после эволюции")

# ---- 3. авария ----
s_before = current_snapshot()
run(f"DELETE FROM {T} WHERE nation_name = 'RUSSIA'", "3. «Авария»: случайно удалили клиентов из RUSSIA")
run(f"SELECT count(*) AS rows, count_if(nation_name = 'RUSSIA') AS russia FROM {T}", "Текущее состояние после DELETE")

# ---- 4. time travel ----
run(f"SELECT count(*) AS rows, count_if(nation_name = 'RUSSIA') AS russia FROM {T} FOR VERSION AS OF {s_before}",
    f"4. Time travel: читаем снапшот до аварии ({s_before})")

# ---- 5. восстановление ----
run(f"ALTER TABLE {T} EXECUTE rollback_to_snapshot({s_before})",
    "5. Восстановление: откат таблицы к снапшоту до аварии")
run(f"SELECT count(*) AS rows, count_if(nation_name = 'RUSSIA') AS russia FROM {T}", "После отката")

# ---- 6. вернуть исходное имя колонки ----
run(f"ALTER TABLE {T} RENAME COLUMN phone_number TO phone",
    "6. Возвращаем имя phone (Gold-скрипты ссылаются на него); customer_tier оставляем как обогащение")
run(f"SELECT column_name, data_type FROM lakekeeper.information_schema.columns "
    f"WHERE table_schema = 'silver' AND table_name = 'customer' ORDER BY ordinal_position", "Итоговая схема")
run(f"SELECT snapshot_id, committed_at, operation FROM {SNAP} ORDER BY committed_at", "Итоговая история снапшотов")
