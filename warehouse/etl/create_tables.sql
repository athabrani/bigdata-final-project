PRAGMA foreign_keys = ON;

BEGIN TRANSACTION;


DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS dim_trend_daily;
DROP TABLE IF EXISTS dim_product_category;
DROP TABLE IF EXISTS dim_date;


CREATE TABLE dim_date (
  date_key     INTEGER PRIMARY KEY,       
  sale_date    TEXT NOT NULL,             
  year         INTEGER NOT NULL,
  month        INTEGER NOT NULL,
  day_of_week  INTEGER NOT NULL,           
  is_weekend   INTEGER NOT NULL             
);


CREATE TABLE dim_product_category (
  product_category_id     INTEGER PRIMARY KEY AUTOINCREMENT,
  product_category_mapped TEXT NOT NULL UNIQUE
);


CREATE TABLE dim_trend_daily (
  trend_id     INTEGER PRIMARY KEY AUTOINCREMENT,
  trend_date   TEXT NOT NULL UNIQUE,        -- YYYY-MM-DD
  coffee       REAL,
  bakery       REAL,
  tea          REAL,
  chocolate    REAL,
  trend_avg    REAL,
  trend_max    REAL
);


CREATE TABLE fact_sales (
  transaction_key    TEXT PRIMARY KEY,      
  date_key           INTEGER NOT NULL,
  product_category_id INTEGER NOT NULL,
  trend_id           INTEGER,               
  unit_price         REAL,
  transaction_qty    REAL,
  gross_revenue      REAL,
  rev_per_unit       REAL,
  trend_for_product  REAL,

  FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
  FOREIGN KEY (product_category_id) REFERENCES dim_product_category(product_category_id),
  FOREIGN KEY (trend_id) REFERENCES dim_trend_daily(trend_id)
);


CREATE INDEX IF NOT EXISTS idx_fact_sales_date_key ON fact_sales(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_category_id ON fact_sales(product_category_id);
CREATE INDEX IF NOT EXISTS idx_dim_trend_daily_date ON dim_trend_daily(trend_date);

COMMIT;
