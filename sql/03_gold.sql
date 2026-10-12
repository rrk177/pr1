-- =====================================================================
-- GOLD — витрины для бизнес-аналитики.
-- Обе таблицы крошечные → PARQUET + ZSTD, без партиционирования (один файл).
-- =====================================================================
SET SESSION lakekeeper.compression_codec = 'ZSTD';

CREATE SCHEMA IF NOT EXISTS lakekeeper.gold;

-- ---------------------------------------------------------------------
-- 1. Ответ на запрос варианта (TPC-H Q10 «Returned Item Reporting»):
--    топ-20 клиентов по выручке от возвращённых позиций за 4-й квартал 1993.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS lakekeeper.gold.q10_returned_items_top20;
CREATE TABLE lakekeeper.gold.q10_returned_items_top20 WITH (format = 'PARQUET') AS
SELECT
    CAST(row_number() OVER (ORDER BY revenue DESC, custkey) AS integer) AS rank,
    '1993-Q4' AS period,
    custkey, name, revenue, acctbal, nation, address, phone, comment
FROM (
    SELECT
        c.custkey,
        c.name,
        sum(ol.net_revenue) AS revenue,
        c.acctbal,
        c.nation_name       AS nation,
        c.address,
        c.phone,
        c.comment
    FROM lakekeeper.silver.order_lines ol
    JOIN lakekeeper.silver.customer    c ON c.custkey = ol.custkey
    WHERE ol.orderdate >= DATE '1993-10-01'
      AND ol.orderdate <  DATE '1994-01-01'
      AND ol.is_returned
    GROUP BY c.custkey, c.name, c.acctbal, c.nation_name, c.address, c.phone, c.comment
    ORDER BY revenue DESC
    LIMIT 20
);

-- ---------------------------------------------------------------------
-- 2. Агрегат для BI-дашборда: возвраты по странам и кварталам.
--    Позволяет увидеть Q10 в динамике (а не только один квартал) и долю возвратов.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS lakekeeper.gold.returns_by_nation_quarter;
CREATE TABLE lakekeeper.gold.returns_by_nation_quarter WITH (format = 'PARQUET') AS
SELECT
    c.region_name,
    c.nation_name,
    CAST(date_trunc('quarter', ol.orderdate) AS date)                      AS order_quarter,
    sum(ol.net_revenue)                                                    AS total_revenue,
    sum(CASE WHEN ol.is_returned THEN ol.net_revenue ELSE 0 END)           AS returned_revenue,
    count(*)                                                               AS lines_cnt,
    count_if(ol.is_returned)                                               AS returned_lines_cnt,
    count(DISTINCT CASE WHEN ol.is_returned THEN ol.custkey END)           AS returning_customers_cnt,
    CAST(sum(CASE WHEN ol.is_returned THEN ol.net_revenue ELSE 0 END)
         / sum(ol.net_revenue) AS decimal(6, 4))                           AS returned_share
FROM lakekeeper.silver.order_lines ol
JOIN lakekeeper.silver.customer    c ON c.custkey = ol.custkey
GROUP BY c.region_name, c.nation_name, CAST(date_trunc('quarter', ol.orderdate) AS date);

SELECT * FROM lakekeeper.gold.q10_returned_items_top20 ORDER BY rank;
