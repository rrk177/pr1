-- =====================================================================
-- ПРОВЕРКИ: качество Silver и корректность Gold.
-- =====================================================================

-- 1. Silver.order_lines: нет дублей по ключу (orderkey, linenumber) и пустых ключей
-- (группируем по orderkey: 15 млн групп влезают в память, 60 млн составных ключей — нет)
SELECT sum(lines)                    AS rows_total,
       sum(lines - distinct_lines)   AS duplicates,
       sum(null_keys)                AS null_keys
FROM (
    SELECT orderkey,
           count(*)                                       AS lines,
           count(DISTINCT linenumber)                     AS distinct_lines,
           count_if(custkey IS NULL OR orderdate IS NULL) AS null_keys
    FROM lakekeeper.silver.order_lines
    GROUP BY orderkey
);

-- 2. Ссылочная целостность: у каждой строки заказа есть клиент в Silver
SELECT count(*) AS orphan_lines
FROM lakekeeper.silver.order_lines ol
LEFT JOIN lakekeeper.silver.customer c ON c.custkey = ol.custkey
WHERE c.custkey IS NULL;

-- 3. Gold vs эталон: исходный Q10 из варианта, выполненный прямо по генератору tpch.sf10.
--    Сравниваем порядок клиентов и выручку (эталон считает в double, Gold — в decimal,
--    поэтому допускаем расхождение < 0.01).
WITH ref AS (
    SELECT row_number() OVER (ORDER BY revenue DESC, custkey) AS rank, custkey, revenue
    FROM (
        SELECT c.custkey, sum(l.extendedprice * (1 - l.discount)) AS revenue
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
SELECT count(*)                                                    AS compared_rows,
       count_if(g.custkey = ref.custkey)                           AS same_customer_at_rank,
       max(abs(CAST(g.revenue AS double) - ref.revenue))           AS max_revenue_diff
FROM lakekeeper.gold.q10_returned_items_top20 g
JOIN ref ON ref.rank = g.rank;
