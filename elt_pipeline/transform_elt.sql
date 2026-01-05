PRAGMA foreign_keys = ON;

BEGIN TRANSACTION;

DROP TABLE IF EXISTS elt_dim_trend_daily;

CREATE TABLE elt_dim_trend_daily AS
WITH base AS (
  SELECT
    DATE(trend_date) AS trend_date,
    AVG(coffee)    AS coffee,
    AVG(bakery)    AS bakery,
    AVG(tea)       AS tea,
    AVG(chocolate) AS chocolate
  FROM raw_trends
  WHERE DATE(trend_date) IS NOT NULL
  GROUP BY DATE(trend_date)
),
final AS (
  SELECT
    trend_date,
    coffee, bakery, tea, chocolate,
    (COALESCE(coffee,0)+COALESCE(bakery,0)+COALESCE(tea,0)+COALESCE(chocolate,0))/4.0 AS trend_avg,
    CASE
      WHEN COALESCE(coffee,0) >= COALESCE(bakery,0)
       AND COALESCE(coffee,0) >= COALESCE(tea,0)
       AND COALESCE(coffee,0) >= COALESCE(chocolate,0) THEN COALESCE(coffee,0)
      WHEN COALESCE(bakery,0) >= COALESCE(tea,0)
       AND COALESCE(bakery,0) >= COALESCE(chocolate,0) THEN COALESCE(bakery,0)
      WHEN COALESCE(tea,0) >= COALESCE(chocolate,0) THEN COALESCE(tea,0)
      ELSE COALESCE(chocolate,0)
    END AS trend_max
  FROM base
)
SELECT * FROM final;


--------------------------------------------------------------------------------
-- B) FACT SALES (clean + parse date + map category + join trend daily)
-- transaction_id, transaction_date, product_category, product_type, unit_price, transaction_qty
--------------------------------------------------------------------------------

DROP TABLE IF EXISTS elt_fact_sales;

CREATE TABLE elt_fact_sales AS
WITH cleaned AS (
  SELECT
    CAST(transaction_id AS TEXT) AS transaction_key,

    -- Normalisasi format tanggal:
    -- 1) Jika "MM/DD/YYYY" -> konversi ke "YYYY-MM-DD"
    -- 2) Else ambil 10 char pertama, asumsi "YYYY-MM-DD..." / "YYYY-MM-DD"
    CASE
      WHEN instr(trim(transaction_date), '/') > 0 THEN
        printf(
          '%04d-%02d-%02d',
          CAST(
            substr(
              substr(trim(transaction_date), instr(trim(transaction_date), '/') + 1),
              instr(substr(trim(transaction_date), instr(trim(transaction_date), '/') + 1), '/') + 1
            ) AS INTEGER
          ),
          CAST(substr(trim(transaction_date), 1, instr(trim(transaction_date), '/') - 1) AS INTEGER),
          CAST(
            substr(
              substr(trim(transaction_date), instr(trim(transaction_date), '/') + 1),
              1,
              instr(substr(trim(transaction_date), instr(trim(transaction_date), '/') + 1), '/') - 1
            ) AS INTEGER
          )
        )
      ELSE
        substr(trim(transaction_date), 1, 10)
    END AS sale_date_str,

    LOWER(COALESCE(product_category,'')) AS product_category_raw,
    LOWER(COALESCE(product_type,''))     AS product_type_raw,
    CAST(unit_price AS REAL)             AS unit_price,
    CAST(transaction_qty AS REAL)        AS transaction_qty
  FROM raw_sales
),
filtered AS (
  SELECT
    transaction_key,
    DATE(sale_date_str) AS sale_date,
    product_category_raw,
    product_type_raw,
    unit_price,
    transaction_qty
  FROM cleaned
  WHERE DATE(sale_date_str) IS NOT NULL
    AND product_category_raw NOT IN ('branded','flavours','flavors','housewares')
    AND product_type_raw NOT IN ('housewares','clothing')
),
mapped AS (
  SELECT
    transaction_key,
    sale_date,
    unit_price,
    transaction_qty,

    CASE
      WHEN product_type_raw LIKE '%coffee%'
        OR product_type_raw LIKE '%espresso%'
        OR product_type_raw LIKE '%latte%'
        OR product_type_raw LIKE '%cappuccino%'
        OR product_type_raw LIKE '%americano%'
        OR product_type_raw LIKE '%macchiato%'
        OR product_type_raw LIKE '%cold brew%'
        OR product_type_raw LIKE '%mocha%'
        OR product_type_raw LIKE '%drip%'
        OR product_type_raw LIKE '%beans%'
      THEN 'coffee'

      WHEN product_type_raw LIKE '%tea%'
        OR product_type_raw LIKE '%chai%'
        OR product_type_raw LIKE '%matcha%'
        OR product_type_raw LIKE '%earl%'
        OR product_type_raw LIKE '%herbal%'
      THEN 'tea'

      WHEN product_type_raw LIKE '%chocolate%'
        OR product_type_raw LIKE '%cocoa%'
      THEN 'chocolate'

      WHEN product_type_raw LIKE '%bakery%'
        OR product_type_raw LIKE '%croissant%'
        OR product_type_raw LIKE '%muffin%'
        OR product_type_raw LIKE '%cookie%'
        OR product_type_raw LIKE '%cake%'
        OR product_type_raw LIKE '%pastry%'
        OR product_type_raw LIKE '%bread%'
        OR product_type_raw LIKE '%donut%'
        OR product_type_raw LIKE '%brownie%'
        OR product_type_raw LIKE '%scone%'
        OR product_type_raw LIKE '%biscotti%'
      THEN 'bakery'

      ELSE NULL
    END AS product_category_mapped
  FROM filtered
),
final_mapped AS (
  SELECT *
  FROM mapped
  WHERE product_category_mapped IS NOT NULL
),
joined AS (
  SELECT
    f.transaction_key,
    f.sale_date,
    CAST(STRFTIME('%Y%m%d', f.sale_date) AS INTEGER) AS date_key,
    CAST(STRFTIME('%Y', f.sale_date) AS INTEGER) AS year,
    CAST(STRFTIME('%m', f.sale_date) AS INTEGER) AS month,
    CAST(STRFTIME('%w', f.sale_date) AS INTEGER) AS day_of_week,
    CASE WHEN CAST(STRFTIME('%w', f.sale_date) AS INTEGER) IN (0,6) THEN 1 ELSE 0 END AS is_weekend,

    f.product_category_mapped,
    f.unit_price,
    f.transaction_qty,

    (COALESCE(f.unit_price,0) * COALESCE(f.transaction_qty,0)) AS gross_revenue,

    CASE
      WHEN COALESCE(f.transaction_qty,0) != 0
      THEN (COALESCE(f.unit_price,0) * COALESCE(f.transaction_qty,0)) / f.transaction_qty
      ELSE 0
    END AS rev_per_unit,

    t.coffee, t.bakery, t.tea, t.chocolate, t.trend_avg, t.trend_max,

    CASE f.product_category_mapped
      WHEN 'coffee' THEN COALESCE(t.coffee, 0)
      WHEN 'bakery' THEN COALESCE(t.bakery, 0)
      WHEN 'tea' THEN COALESCE(t.tea, 0)
      WHEN 'chocolate' THEN COALESCE(t.chocolate, 0)
      ELSE COALESCE(t.trend_avg, 0)
    END AS trend_for_product
  FROM final_mapped f
  LEFT JOIN elt_dim_trend_daily t
    ON t.trend_date = f.sale_date
)
SELECT *
FROM joined
WHERE date_key IS NOT NULL AND date_key != 0;

--------------------------------------------------------------------------------
-- C) DIM DATE & DIM PRODUCT CATEGORY (diturunkan dari FACT)
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS elt_dim_date;
CREATE TABLE elt_dim_date AS
SELECT DISTINCT
  date_key, sale_date, year, month, day_of_week, is_weekend
FROM elt_fact_sales
ORDER BY date_key;

DROP TABLE IF EXISTS elt_dim_product_category;
CREATE TABLE elt_dim_product_category AS
SELECT DISTINCT
  product_category_mapped
FROM elt_fact_sales
ORDER BY product_category_mapped;

COMMIT;
