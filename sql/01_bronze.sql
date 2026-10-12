-- =====================================================================
-- BRONZE — сырой слой. Вариант 2 (TPC-H Q10), масштаб sf10.
-- Таблицы забираются из генератора tpch.sf10 «как есть»: все колонки, исходные типы,
-- без фильтров. Добавлены только техколонки происхождения (_ingested_at, _source),
-- чтобы по любой строке было видно, откуда и когда она пришла.
-- Формат PARQUET + кодек ZSTD — победитель бенчмарка (см. results/, отчет.md §5).
-- Партиционирования нет: Bronze загружается одним снапшотом и читается целиком
-- при построении Silver, ключа инкрементальной загрузки у TPC-H нет.
-- =====================================================================
SET SESSION lakekeeper.compression_codec = 'ZSTD';

CREATE SCHEMA IF NOT EXISTS lakekeeper.bronze;

DROP TABLE IF EXISTS lakekeeper.bronze.nation;
CREATE TABLE lakekeeper.bronze.nation WITH (format = 'PARQUET') AS
SELECT *, CAST(current_timestamp AS timestamp(6) with time zone) AS _ingested_at,
       'tpch.sf10.nation' AS _source
FROM tpch.sf10.nation;

DROP TABLE IF EXISTS lakekeeper.bronze.region;
CREATE TABLE lakekeeper.bronze.region WITH (format = 'PARQUET') AS
SELECT *, CAST(current_timestamp AS timestamp(6) with time zone) AS _ingested_at,
       'tpch.sf10.region' AS _source
FROM tpch.sf10.region;

DROP TABLE IF EXISTS lakekeeper.bronze.customer;
CREATE TABLE lakekeeper.bronze.customer WITH (format = 'PARQUET') AS
SELECT *, CAST(current_timestamp AS timestamp(6) with time zone) AS _ingested_at,
       'tpch.sf10.customer' AS _source
FROM tpch.sf10.customer;

DROP TABLE IF EXISTS lakekeeper.bronze.orders;
CREATE TABLE lakekeeper.bronze.orders WITH (format = 'PARQUET') AS
SELECT *, CAST(current_timestamp AS timestamp(6) with time zone) AS _ingested_at,
       'tpch.sf10.orders' AS _source
FROM tpch.sf10.orders;

DROP TABLE IF EXISTS lakekeeper.bronze.lineitem;
CREATE TABLE lakekeeper.bronze.lineitem WITH (format = 'PARQUET') AS
SELECT *, CAST(current_timestamp AS timestamp(6) with time zone) AS _ingested_at,
       'tpch.sf10.lineitem' AS _source
FROM tpch.sf10.lineitem;

-- контроль: число строк в Bronze должно совпасть с источником
SELECT 'customer' AS tbl, (SELECT count(*) FROM lakekeeper.bronze.customer) AS bronze_rows, (SELECT count(*) FROM tpch.sf10.customer) AS source_rows
UNION ALL SELECT 'orders',   (SELECT count(*) FROM lakekeeper.bronze.orders),   (SELECT count(*) FROM tpch.sf10.orders)
UNION ALL SELECT 'lineitem', (SELECT count(*) FROM lakekeeper.bronze.lineitem), (SELECT count(*) FROM tpch.sf10.lineitem)
UNION ALL SELECT 'nation',   (SELECT count(*) FROM lakekeeper.bronze.nation),   (SELECT count(*) FROM tpch.sf10.nation)
UNION ALL SELECT 'region',   (SELECT count(*) FROM lakekeeper.bronze.region),   (SELECT count(*) FROM tpch.sf10.region);
