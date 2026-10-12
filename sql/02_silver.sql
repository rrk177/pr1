-- =====================================================================
-- SILVER — очищенный, нормализованный и обогащённый слой.
--   * очистка: trim строк, отбрасывание строк с пустыми ключами и невалидными
--     значениями (quantity/price <= 0, discount вне [0;0.10], отгрузка раньше заказа),
--     дедупликация клиентов по custkey;
--   * нормализация: деньги double → decimal(12,2) (точная арифметика вместо
--     накопления ошибок плавающей точки), скидка/налог → decimal(4,2), флаги → upper();
--   * обогащение: клиент получает имя страны и региона (customer ⋈ nation ⋈ region),
--     строка заказа — чистую выручку net_revenue = extendedprice * (1 - discount)
--     и признак возврата is_returned.
-- Колонки-«комментарии» lineitem/orders и неиспользуемые атрибуты в Silver не несём.
-- =====================================================================
SET SESSION lakekeeper.compression_codec = 'ZSTD';

CREATE SCHEMA IF NOT EXISTS lakekeeper.silver;

-- ---------------------------------------------------------------------
-- 1. Клиенты, обогащённые страной и регионом (1.5 млн строк, ~100 МБ).
--    Без партиционирования: небольшая таблица-измерение, её всегда читают
--    целиком для джойна; партиции дали бы только мелкие файлы.
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS lakekeeper.silver.customer;
CREATE TABLE lakekeeper.silver.customer WITH (format = 'PARQUET') AS
WITH dedup AS (
    SELECT *, row_number() OVER (PARTITION BY custkey ORDER BY _ingested_at DESC) AS rn
    FROM lakekeeper.bronze.customer
    WHERE custkey IS NOT NULL AND nationkey IS NOT NULL
)
SELECT
    c.custkey,
    trim(c.name)                        AS name,
    trim(c.address)                     AS address,
    trim(c.phone)                       AS phone,
    CAST(c.acctbal AS decimal(12, 2))   AS acctbal,
    upper(trim(c.mktsegment))           AS mktsegment,
    trim(c.comment)                     AS comment,
    c.nationkey,
    trim(n.name)                        AS nation_name,
    trim(r.name)                        AS region_name
FROM dedup c
JOIN lakekeeper.bronze.nation n ON n.nationkey = c.nationkey
JOIN lakekeeper.bronze.region r ON r.regionkey = n.regionkey
WHERE c.rn = 1;

-- ---------------------------------------------------------------------
-- 2. Факт «строки заказов» = orders ⋈ lineitem (≈60 млн строк).
--    Партиционирование month(orderdate): аналитика по возвратам (и Q10) режет
--    данные по периоду заказа → Trino читает только нужные месячные партиции
--    (квартал Q10 = 3 партиции из ~80). Месяц, а не год: при годовой партиции
--    Q10 читал бы в 4 раза больше данных, а файлы по месяцу (~25 МБ) ещё не «мелкие».
-- ---------------------------------------------------------------------
DROP TABLE IF EXISTS lakekeeper.silver.order_lines;
-- Пустая таблица нужной схемы, затем загрузка порциями по году заказа: джойн
-- 60 млн lineitem × 15 млн orders одним запросом не влезает в лимит памяти Trino
-- (1.87 ГБ на узел), а по году хеш-таблица заказов в ~7 раз меньше.
CREATE TABLE lakekeeper.silver.order_lines
WITH (format = 'PARQUET', partitioning = ARRAY['month(orderdate)']) AS
SELECT
    l.orderkey,
    l.linenumber,
    o.custkey,
    o.orderdate,
    upper(trim(o.orderstatus))                                   AS orderstatus,
    trim(o.orderpriority)                                        AS orderpriority,
    l.partkey,
    l.suppkey,
    CAST(l.quantity      AS decimal(12, 2))                      AS quantity,
    CAST(l.extendedprice AS decimal(12, 2))                      AS extendedprice,
    CAST(l.discount      AS decimal(4, 2))                       AS discount,
    CAST(l.tax           AS decimal(4, 2))                       AS tax,
    CAST(l.extendedprice AS decimal(12, 2))
        * (1 - CAST(l.discount AS decimal(4, 2)))                AS net_revenue,
    upper(trim(l.returnflag))                                    AS returnflag,
    upper(trim(l.returnflag)) = 'R'                              AS is_returned,
    upper(trim(l.linestatus))                                    AS linestatus,
    l.shipdate,
    l.commitdate,
    l.receiptdate,
    upper(trim(l.shipmode))                                      AS shipmode
FROM lakekeeper.bronze.lineitem l
JOIN lakekeeper.bronze.orders o ON o.orderkey = l.orderkey
WHERE l.orderkey IS NOT NULL
  AND o.custkey  IS NOT NULL
  AND l.quantity > 0
  AND l.extendedprice > 0
  AND l.discount BETWEEN 0 AND 0.10
  AND l.shipdate >= o.orderdate
WITH NO DATA;

INSERT INTO lakekeeper.silver.order_lines
SELECT
    l.orderkey,
    l.linenumber,
    o.custkey,
    o.orderdate,
    upper(trim(o.orderstatus))                                   AS orderstatus,
    trim(o.orderpriority)                                        AS orderpriority,
    l.partkey,
    l.suppkey,
    CAST(l.quantity      AS decimal(12, 2))                      AS quantity,
    CAST(l.extendedprice AS decimal(12, 2))                      AS extendedprice,
    CAST(l.discount      AS decimal(4, 2))                       AS discount,
    CAST(l.tax           AS decimal(4, 2))                       AS tax,
    CAST(l.extendedprice AS decimal(12, 2))
        * (1 - CAST(l.discount AS decimal(4, 2)))                AS net_revenue,
    upper(trim(l.returnflag))                                    AS returnflag,
    upper(trim(l.returnflag)) = 'R'                              AS is_returned,
    upper(trim(l.linestatus))                                    AS linestatus,
    l.shipdate,
    l.commitdate,
    l.receiptdate,
    upper(trim(l.shipmode))                                      AS shipmode
FROM lakekeeper.bronze.lineitem l
JOIN lakekeeper.bronze.orders o ON o.orderkey = l.orderkey
WHERE l.orderkey IS NOT NULL
  AND o.custkey  IS NOT NULL
  AND l.quantity > 0
  AND l.extendedprice > 0
  AND l.discount BETWEEN 0 AND 0.10
  AND l.shipdate >= o.orderdate
  AND o.orderdate >= DATE '1992-01-01' AND o.orderdate < DATE '1993-01-01';

INSERT INTO lakekeeper.silver.order_lines
SELECT
    l.orderkey,
    l.linenumber,
    o.custkey,
    o.orderdate,
    upper(trim(o.orderstatus))                                   AS orderstatus,
    trim(o.orderpriority)                                        AS orderpriority,
    l.partkey,
    l.suppkey,
    CAST(l.quantity      AS decimal(12, 2))                      AS quantity,
    CAST(l.extendedprice AS decimal(12, 2))                      AS extendedprice,
    CAST(l.discount      AS decimal(4, 2))                       AS discount,
    CAST(l.tax           AS decimal(4, 2))                       AS tax,
    CAST(l.extendedprice AS decimal(12, 2))
        * (1 - CAST(l.discount AS decimal(4, 2)))                AS net_revenue,
    upper(trim(l.returnflag))                                    AS returnflag,
    upper(trim(l.returnflag)) = 'R'                              AS is_returned,
    upper(trim(l.linestatus))                                    AS linestatus,
    l.shipdate,
    l.commitdate,
    l.receiptdate,
    upper(trim(l.shipmode))                                      AS shipmode
FROM lakekeeper.bronze.lineitem l
JOIN lakekeeper.bronze.orders o ON o.orderkey = l.orderkey
WHERE l.orderkey IS NOT NULL
  AND o.custkey  IS NOT NULL
  AND l.quantity > 0
  AND l.extendedprice > 0
  AND l.discount BETWEEN 0 AND 0.10
  AND l.shipdate >= o.orderdate
  AND o.orderdate >= DATE '1993-01-01' AND o.orderdate < DATE '1994-01-01';

INSERT INTO lakekeeper.silver.order_lines
SELECT
    l.orderkey,
    l.linenumber,
    o.custkey,
    o.orderdate,
    upper(trim(o.orderstatus))                                   AS orderstatus,
    trim(o.orderpriority)                                        AS orderpriority,
    l.partkey,
    l.suppkey,
    CAST(l.quantity      AS decimal(12, 2))                      AS quantity,
    CAST(l.extendedprice AS decimal(12, 2))                      AS extendedprice,
    CAST(l.discount      AS decimal(4, 2))                       AS discount,
    CAST(l.tax           AS decimal(4, 2))                       AS tax,
    CAST(l.extendedprice AS decimal(12, 2))
        * (1 - CAST(l.discount AS decimal(4, 2)))                AS net_revenue,
    upper(trim(l.returnflag))                                    AS returnflag,
    upper(trim(l.returnflag)) = 'R'                              AS is_returned,
    upper(trim(l.linestatus))                                    AS linestatus,
    l.shipdate,
    l.commitdate,
    l.receiptdate,
    upper(trim(l.shipmode))                                      AS shipmode
FROM lakekeeper.bronze.lineitem l
JOIN lakekeeper.bronze.orders o ON o.orderkey = l.orderkey
WHERE l.orderkey IS NOT NULL
  AND o.custkey  IS NOT NULL
  AND l.quantity > 0
  AND l.extendedprice > 0
  AND l.discount BETWEEN 0 AND 0.10
  AND l.shipdate >= o.orderdate
  AND o.orderdate >= DATE '1994-01-01' AND o.orderdate < DATE '1995-01-01';

INSERT INTO lakekeeper.silver.order_lines
SELECT
    l.orderkey,
    l.linenumber,
    o.custkey,
    o.orderdate,
    upper(trim(o.orderstatus))                                   AS orderstatus,
    trim(o.orderpriority)                                        AS orderpriority,
    l.partkey,
    l.suppkey,
    CAST(l.quantity      AS decimal(12, 2))                      AS quantity,
    CAST(l.extendedprice AS decimal(12, 2))                      AS extendedprice,
    CAST(l.discount      AS decimal(4, 2))                       AS discount,
    CAST(l.tax           AS decimal(4, 2))                       AS tax,
    CAST(l.extendedprice AS decimal(12, 2))
        * (1 - CAST(l.discount AS decimal(4, 2)))                AS net_revenue,
    upper(trim(l.returnflag))                                    AS returnflag,
    upper(trim(l.returnflag)) = 'R'                              AS is_returned,
    upper(trim(l.linestatus))                                    AS linestatus,
    l.shipdate,
    l.commitdate,
    l.receiptdate,
    upper(trim(l.shipmode))                                      AS shipmode
FROM lakekeeper.bronze.lineitem l
JOIN lakekeeper.bronze.orders o ON o.orderkey = l.orderkey
WHERE l.orderkey IS NOT NULL
  AND o.custkey  IS NOT NULL
  AND l.quantity > 0
  AND l.extendedprice > 0
  AND l.discount BETWEEN 0 AND 0.10
  AND l.shipdate >= o.orderdate
  AND o.orderdate >= DATE '1995-01-01' AND o.orderdate < DATE '1996-01-01';

INSERT INTO lakekeeper.silver.order_lines
SELECT
    l.orderkey,
    l.linenumber,
    o.custkey,
    o.orderdate,
    upper(trim(o.orderstatus))                                   AS orderstatus,
    trim(o.orderpriority)                                        AS orderpriority,
    l.partkey,
    l.suppkey,
    CAST(l.quantity      AS decimal(12, 2))                      AS quantity,
    CAST(l.extendedprice AS decimal(12, 2))                      AS extendedprice,
    CAST(l.discount      AS decimal(4, 2))                       AS discount,
    CAST(l.tax           AS decimal(4, 2))                       AS tax,
    CAST(l.extendedprice AS decimal(12, 2))
        * (1 - CAST(l.discount AS decimal(4, 2)))                AS net_revenue,
    upper(trim(l.returnflag))                                    AS returnflag,
    upper(trim(l.returnflag)) = 'R'                              AS is_returned,
    upper(trim(l.linestatus))                                    AS linestatus,
    l.shipdate,
    l.commitdate,
    l.receiptdate,
    upper(trim(l.shipmode))                                      AS shipmode
FROM lakekeeper.bronze.lineitem l
JOIN lakekeeper.bronze.orders o ON o.orderkey = l.orderkey
WHERE l.orderkey IS NOT NULL
  AND o.custkey  IS NOT NULL
  AND l.quantity > 0
  AND l.extendedprice > 0
  AND l.discount BETWEEN 0 AND 0.10
  AND l.shipdate >= o.orderdate
  AND o.orderdate >= DATE '1996-01-01' AND o.orderdate < DATE '1997-01-01';

INSERT INTO lakekeeper.silver.order_lines
SELECT
    l.orderkey,
    l.linenumber,
    o.custkey,
    o.orderdate,
    upper(trim(o.orderstatus))                                   AS orderstatus,
    trim(o.orderpriority)                                        AS orderpriority,
    l.partkey,
    l.suppkey,
    CAST(l.quantity      AS decimal(12, 2))                      AS quantity,
    CAST(l.extendedprice AS decimal(12, 2))                      AS extendedprice,
    CAST(l.discount      AS decimal(4, 2))                       AS discount,
    CAST(l.tax           AS decimal(4, 2))                       AS tax,
    CAST(l.extendedprice AS decimal(12, 2))
        * (1 - CAST(l.discount AS decimal(4, 2)))                AS net_revenue,
    upper(trim(l.returnflag))                                    AS returnflag,
    upper(trim(l.returnflag)) = 'R'                              AS is_returned,
    upper(trim(l.linestatus))                                    AS linestatus,
    l.shipdate,
    l.commitdate,
    l.receiptdate,
    upper(trim(l.shipmode))                                      AS shipmode
FROM lakekeeper.bronze.lineitem l
JOIN lakekeeper.bronze.orders o ON o.orderkey = l.orderkey
WHERE l.orderkey IS NOT NULL
  AND o.custkey  IS NOT NULL
  AND l.quantity > 0
  AND l.extendedprice > 0
  AND l.discount BETWEEN 0 AND 0.10
  AND l.shipdate >= o.orderdate
  AND o.orderdate >= DATE '1997-01-01' AND o.orderdate < DATE '1998-01-01';

INSERT INTO lakekeeper.silver.order_lines
SELECT
    l.orderkey,
    l.linenumber,
    o.custkey,
    o.orderdate,
    upper(trim(o.orderstatus))                                   AS orderstatus,
    trim(o.orderpriority)                                        AS orderpriority,
    l.partkey,
    l.suppkey,
    CAST(l.quantity      AS decimal(12, 2))                      AS quantity,
    CAST(l.extendedprice AS decimal(12, 2))                      AS extendedprice,
    CAST(l.discount      AS decimal(4, 2))                       AS discount,
    CAST(l.tax           AS decimal(4, 2))                       AS tax,
    CAST(l.extendedprice AS decimal(12, 2))
        * (1 - CAST(l.discount AS decimal(4, 2)))                AS net_revenue,
    upper(trim(l.returnflag))                                    AS returnflag,
    upper(trim(l.returnflag)) = 'R'                              AS is_returned,
    upper(trim(l.linestatus))                                    AS linestatus,
    l.shipdate,
    l.commitdate,
    l.receiptdate,
    upper(trim(l.shipmode))                                      AS shipmode
FROM lakekeeper.bronze.lineitem l
JOIN lakekeeper.bronze.orders o ON o.orderkey = l.orderkey
WHERE l.orderkey IS NOT NULL
  AND o.custkey  IS NOT NULL
  AND l.quantity > 0
  AND l.extendedprice > 0
  AND l.discount BETWEEN 0 AND 0.10
  AND l.shipdate >= o.orderdate
  AND o.orderdate >= DATE '1998-01-01' AND o.orderdate < DATE '1999-01-01';


-- контроль: сколько строк отсеяно очисткой
SELECT 'customer' AS tbl,
       (SELECT count(*) FROM lakekeeper.bronze.customer) AS bronze_rows,
       (SELECT count(*) FROM lakekeeper.silver.customer) AS silver_rows
UNION ALL
SELECT 'order_lines',
       (SELECT count(*) FROM lakekeeper.bronze.lineitem),
       (SELECT count(*) FROM lakekeeper.silver.order_lines);
