-- =====================================================================
-- ПРОВЕРКИ: качество Silver и корректность Gold.
-- =====================================================================

-- 1. Silver.order_lines: нет дублей по ключу (orderkey, linenumber) и пустых ключей
-- (группируем по orderkey: 15 млн групп влезают в память, 60 млн составных ключей — нет;
--  различные linenumber (1..7) считаем битовой маской — компактнее, чем count(DISTINCT))
SELECT sum(lines)                    AS rows_total,
       sum(lines - distinct_lines)   AS duplicates,
       sum(null_keys)                AS null_keys
FROM (
    SELECT orderkey,
           count(*)                                       AS lines,
           bit_count(bitwise_or_agg(bitwise_left_shift(1, linenumber)), 64) AS distinct_lines,
           count_if(custkey IS NULL OR orderdate IS NULL) AS null_keys
    FROM lakekeeper.silver.order_lines
    GROUP BY orderkey
);

-- 2. Ссылочная целостность: у каждой строки заказа есть клиент в Silver
SELECT count(*) AS orphan_lines
FROM lakekeeper.silver.order_lines ol
LEFT JOIN lakekeeper.silver.customer c ON c.custkey = ol.custkey
WHERE c.custkey IS NULL;

-- 3. Gold vs эталон: запрос варианта 2 ДОСЛОВНО (заменён только sf1 → sf10, как требует вариант),
--    выполненный прямо по генератору tpch.sf10. Сравниваем все 8 колонок вывода построчно по месту в топе.
--    Эталон считает деньги в double, Gold — в decimal, поэтому revenue/acctbal сравниваем с допуском 0.01.
--    Silver делает trim() строк, а у части клиентов TPC-H comment/address начинается или кончается пробелом,
--    поэтому строки сравниваем дважды: побайтно и после trim эталона (разница только в крайних пробелах).
WITH ref AS (
    SELECT row_number() OVER (ORDER BY revenue DESC) AS rank, *
    FROM (
        SELECT
          c.custkey,
          c.name,
          sum(l.extendedprice * (1 - l.discount)) AS revenue,
          c.acctbal,
          n.name AS nation,
          c.address,
          c.phone,
          c.comment
        FROM tpch.sf10.customer AS c
        JOIN tpch.sf10.orders   AS o ON c.custkey = o.custkey
        JOIN tpch.sf10.lineitem AS l ON l.orderkey = o.orderkey
        JOIN tpch.sf10.nation   AS n ON c.nationkey = n.nationkey
        WHERE o.orderdate >= DATE '1993-10-01'
          AND o.orderdate <  DATE '1994-01-01'
          AND l.returnflag = 'R'
        GROUP BY c.custkey, c.name, c.acctbal, n.name, c.address, c.phone, c.comment
        ORDER BY revenue DESC
        LIMIT 20
    )
)
SELECT count(*)                                                          AS compared_rows,
       count_if(g.custkey = ref.custkey AND g.nation = ref.nation AND g.phone = ref.phone
                AND abs(CAST(g.acctbal AS double) - ref.acctbal) < 0.01
                AND abs(CAST(g.revenue AS double) - ref.revenue) < 0.01) AS same_customer_revenue,
       count_if(g.name = ref.name AND g.address = ref.address
                AND g.comment = ref.comment)                             AS text_byte_equal,
       count_if(g.name = trim(ref.name) AND g.address = trim(ref.address)
                AND g.comment = trim(ref.comment))                       AS text_equal_after_trim,
       max(abs(CAST(g.revenue AS double) - ref.revenue))                 AS max_revenue_diff
FROM lakekeeper.gold.q10_returned_items_top20 g
JOIN ref ON ref.rank = g.rank;
