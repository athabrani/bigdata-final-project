import sqlite3
from pathlib import Path
import time, json

WAREHOUSE_DIR = BASE_DIR / "warehouse"
WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = WAREHOUSE_DIR / "coffee_dw.sqlite"

LOG_DIR = BASE_DIR / "logs" / "etl"
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOAD_LOG = LOG_DIR / "load_log.jsonl"


def append_jsonl(path: Path, record: dict):
    record = dict(record)
    record["timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def create_star_schema(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("PRAGMA foreign_keys = ON;")

    # Drop lama (agar rerun aman)
    cur.executescript("""
    DROP TABLE IF EXISTS fact_sales;
    DROP TABLE IF EXISTS dim_trend;
    DROP TABLE IF EXISTS dim_product_category;
    DROP TABLE IF EXISTS dim_date;
    """)

    # Dim Date
    cur.executescript("""
    CREATE TABLE dim_date (
        date_key INTEGER PRIMARY KEY,
        sale_date TEXT,
        year INTEGER,
        month INTEGER,
        day_of_week INTEGER,
        is_weekend INTEGER
    );
    """)

    # Dim Product Category (mapped)
    cur.executescript("""
    CREATE TABLE dim_product_category (
        product_category_id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_category_mapped TEXT UNIQUE
    );
    """)

    # Dim Trend (kombinasi nilai trend per tanggal)
    cur.executescript("""
    CREATE TABLE dim_trend (
        trend_id INTEGER PRIMARY KEY AUTOINCREMENT,
        coffee REAL,
        bakery REAL,
        tea REAL,
        chocolate REAL,
        trend_avg REAL,
        trend_max REAL,
        UNIQUE (coffee, bakery, tea, chocolate, trend_avg, trend_max)
    );
    """)

    # Fact Sales
    cur.executescript("""
    CREATE TABLE fact_sales (
        transaction_key TEXT PRIMARY KEY,
        date_key INTEGER,
        product_category_id INTEGER,
        trend_id INTEGER,
        gross_revenue REAL,
        rev_per_unit REAL,
        trend_for_product REAL,
        FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
        FOREIGN KEY (product_category_id) REFERENCES dim_product_category(product_category_id),
        FOREIGN KEY (trend_id) REFERENCES dim_trend(trend_id)
    );
    """)
    cur.executescript("""
    DROP TABLE IF EXISTS mart_daily_category_sales;
    CREATE TABLE mart_daily_category_sales (
        sale_date TEXT,
        product_category TEXT,
        year INTEGER,
        month INTEGER,
        day_of_week INTEGER,
        is_weekend INTEGER,
        n_transactions INTEGER,
        total_qty REAL,
        daily_revenue REAL,
        avg_revenue_per_tx REAL,
        avg_trend_for_product REAL,
        trend_avg_overall REAL,
        trend_max_overall REAL
    );
    """)
    conn.commit()


def prepare_dim_fact(df_transformed: pd.DataFrame):
    df = df_transformed.copy()

    # Tentukan transaction_key
    if "transaction_id" in df.columns:
        df["transaction_key"] = df["transaction_id"].astype(str)
    elif "transaction_sk" in df.columns:
        df["transaction_key"] = df["transaction_sk"].astype(str)
    else:
        # fallback terakhir
        df["transaction_key"] = pd.util.hash_pandas_object(df, index=False).astype("int64").astype(str)

    # Pastikan kolom minimal ada
    needed_cols = ["date_key", "year", "month", "day_of_week", "is_weekend",
                   "gross_revenue", "rev_per_unit", "trend_for_product", "product_category_mapped"]
    for c in needed_cols:
        if c not in df.columns:
            df[c] = 0

    # Dim Date
    # sale_date bisa dihitung dari date_key supaya konsisten
    df["sale_date"] = df["date_key"].astype(str)
    df["sale_date"] = pd.to_datetime(df["sale_date"], format="%Y%m%d", errors="coerce").dt.strftime("%Y-%m-%d")

    dim_date = (
        df[["date_key", "sale_date", "year", "month", "day_of_week", "is_weekend"]]
        .drop_duplicates(subset=["date_key"])
        .sort_values("date_key")
    )

    # Dim Product Category
    dim_product_category = (
        df[["product_category_mapped"]]
        .fillna("other")
        .drop_duplicates()
        .sort_values("product_category_mapped")
    )

    # Dim Trend
    trend_cols = ["coffee", "bakery", "tea", "chocolate", "trend_avg", "trend_max"]
    for c in trend_cols:
        if c not in df.columns:
            df[c] = np.nan

    dim_trend = (
        df[trend_cols]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    fact_sales = df[[
        "transaction_key", "date_key", "product_category_mapped",
        "gross_revenue", "rev_per_unit", "trend_for_product"
    ] + trend_cols].copy()

    return dim_date, dim_product_category, dim_trend, fact_sales


def load_to_sqlite(df_transformed: pd.DataFrame, db_path: Path) -> Path:
    t0 = time.time()
    status = "success"
    message = None

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")

    try:
        # 1) Create schema
        create_star_schema(conn)

        # 2) Prepare dims/facts
        dim_date, dim_product_category, dim_trend, fact_sales = prepare_dim_fact(df_transformed)

        # 3) Load dim tables
        dim_date.to_sql("dim_date", conn, if_exists="append", index=False)
        dim_product_category.to_sql("dim_product_category", conn, if_exists="append", index=False)
        dim_trend.to_sql("dim_trend", conn, if_exists="append", index=False)

        # 4) Build lookup maps
        # product_category_id
        prod_map = pd.read_sql_query("SELECT product_category_id, product_category_mapped FROM dim_product_category", conn)
        fact_sales = fact_sales.merge(prod_map, on="product_category_mapped", how="left")

        # trend_id by matching exact values (dim_trend uniqueness)
        trend_map = pd.read_sql_query("""
            SELECT trend_id, coffee, bakery, tea, chocolate, trend_avg, trend_max
            FROM dim_trend
        """, conn)

        fact_sales = fact_sales.merge(trend_map, on=["coffee","bakery","tea","chocolate","trend_avg","trend_max"], how="left")

        # 5) Final fact
        final_fact = fact_sales[[
            "transaction_key", "date_key", "product_category_id", "trend_id",
            "gross_revenue", "rev_per_unit", "trend_for_product"
        ]].copy()

        # Rename transaction_key -> match DDL
        final_fact = final_fact.rename(columns={"transaction_key": "transaction_key"})

        # Pastikan tidak ada FK null (perbaikan agar load tidak gagal)
        final_fact["product_category_id"] = final_fact["product_category_id"].fillna(
            prod_map.loc[prod_map["product_category_mapped"]=="other","product_category_id"].iloc[0]
            if (prod_map["product_category_mapped"]=="other").any()
            else prod_map["product_category_id"].min()
        ).astype(int)

        # jika trend_id null, set ke 1 (atau buat default trend row; ini minimum fix untuk load)
        final_fact["trend_id"] = final_fact["trend_id"].fillna(1).astype(int)

        # Load fact
        final_fact.to_sql("fact_sales", conn, if_exists="append", index=False)

        df_daily_category.to_sql( "mart_daily_category_sales", conn, if_exists="append", index=False )

        conn.commit()

        exec_s = time.time() - t0

        append_jsonl(LOAD_LOG, {
            "stage": "load",
            "db_path": str(db_path),
            "status": status,
            "message": message,
            "tables_loaded": {
                "dim_date": int(dim_date.shape[0]),
                "dim_product_category": int(dim_product_category.shape[0]),
                "dim_trend": int(dim_trend.shape[0]),
                "fact_sales": int(final_fact.shape[0]),
            },
            "exec_seconds": round(exec_s, 4),
            "db_size_bytes": int(db_path.stat().st_size) if db_path.exists() else None
        })

    except Exception as e:
        status = "failed"
        message = str(e)
        exec_s = time.time() - t0
        append_jsonl(LOAD_LOG, {
            "stage": "load",
            "db_path": str(db_path),
            "status": status,
            "message": message,
            "exec_seconds": round(exec_s, 4),
        })
        raise
    finally:
        conn.close()

    return db_path


db_path = load_to_sqlite(df_transformed, DB_PATH)
db_path


from IPython.display import display

conn = sqlite3.connect(DB_PATH)
conn.execute("PRAGMA foreign_keys = ON;")

queries = {
    "Q1_total_revenue": """
        SELECT ROUND(SUM(gross_revenue), 2) AS total_revenue
        FROM fact_sales;
    """,
    "Q2_revenue_by_month": """
        SELECT
          d.year,
          d.month,
          COUNT(f.transaction_key) AS n_transactions,
          ROUND(SUM(f.gross_revenue), 2) AS revenue
        FROM fact_sales f
        JOIN dim_date d ON f.date_key = d.date_key
        GROUP BY d.year, d.month
        ORDER BY d.year, d.month;
    """,
    "Q3_revenue_by_category": """
        SELECT
          p.product_category_mapped,
          COUNT(f.transaction_key) AS n_transactions,
          ROUND(SUM(f.gross_revenue), 2) AS revenue,
          ROUND(AVG(f.gross_revenue),2) AS avg_revenue_per_tx
        FROM fact_sales f
        JOIN dim_product_category p ON f.product_category_id = p.product_category_id
        GROUP BY p.product_category_mapped
        ORDER BY revenue DESC;
    """,
    "Q4_avg_rev_per_unit_by_category": """
        SELECT p.product_category_mapped, ROUND(AVG(f.rev_per_unit), 3) AS avg_rev_per_unit
        FROM fact_sales f
        JOIN dim_product_category p ON f.product_category_id = p.product_category_id
        GROUP BY p.product_category_mapped
        ORDER BY avg_rev_per_unit DESC;
    """,
    "Q5_weekend_vs_weekday_revenue": """
        SELECT
          d.is_weekend,
          COUNT(f.transaction_key) AS n_transactions,
          ROUND(SUM(f.gross_revenue),2) AS revenue,
          ROUND(AVG(f.gross_revenue),2) AS avg_revenue_per_tx
        FROM fact_sales f
        JOIN dim_date d ON f.date_key = d.date_key
        GROUP BY d.is_weekend
        ORDER BY d.is_weekend;
    """,
    "Q6_top_10_days_revenue": """
        SELECT d.sale_date, ROUND(SUM(f.gross_revenue), 2) AS revenue
        FROM fact_sales f
        JOIN dim_date d ON f.date_key = d.date_key
        GROUP BY d.sale_date
        ORDER BY revenue DESC
        LIMIT 10;
    """,
    "Q7_trend_vs_revenue_daily": """
        SELECT
          d.sale_date,
          COUNT(f.transaction_key) AS n_transactions,
          COUNT(DISTINCT f.transaction_key) AS n_unique_tx,
          ROUND(SUM(f.gross_revenue),2) AS daily_revenue,
          ROUND(AVG(f.trend_for_product),2) AS avg_trend_for_product
        FROM fact_sales f
        JOIN dim_date d ON f.date_key = d.date_key
        GROUP BY d.sale_date
        ORDER BY d.sale_date;
    """,
    "Q8_category_trend_avg": """
        SELECT p.product_category_mapped,
               ROUND(AVG(f.trend_for_product), 2) AS avg_trend_for_product,
               ROUND(SUM(f.gross_revenue), 2) AS revenue
        FROM fact_sales f
        JOIN dim_product_category p ON f.product_category_id = p.product_category_id
        GROUP BY p.product_category_mapped
        ORDER BY avg_trend_for_product DESC;
    """,
    "Q9_join_integrity_check": """
        SELECT
            SUM(CASE WHEN d.date_key IS NULL THEN 1 ELSE 0 END) AS missing_date_fk,
            SUM(CASE WHEN p.product_category_id IS NULL THEN 1 ELSE 0 END) AS missing_category_fk,
            SUM(CASE WHEN t.trend_id IS NULL THEN 1 ELSE 0 END) AS missing_trend_fk
        FROM fact_sales f
        LEFT JOIN dim_date d ON f.date_key = d.date_key
        LEFT JOIN dim_product_category p ON f.product_category_id = p.product_category_id
        LEFT JOIN dim_trend t ON f.trend_id = t.trend_id;
    """,
    "Q10_monthly_category_share": """
        SELECT d.year, d.month, p.product_category_mapped,
               ROUND(SUM(f.gross_revenue), 2) AS revenue
        FROM fact_sales f
        JOIN dim_date d ON f.date_key = d.date_key
        JOIN dim_product_category p ON f.product_category_id = p.product_category_id
        GROUP BY d.year, d.month, p.product_category_mapped
        ORDER BY d.year, d.month, revenue DESC;
    """
}

results = {}
for name, q in queries.items():
    results[name] = pd.read_sql_query(q, conn)


# tampilkan beberapa hasil
display(results["Q1_total_revenue"]), display(results["Q2_revenue_by_month"]), display(results["Q3_revenue_by_category"])

print("=== LOAD LOG (last line) ===")
!tail -n 1 /logs/etl/load_log.jsonl


tables = pd.read_sql_query("""
SELECT name
FROM sqlite_master
WHERE type='table'
ORDER BY name;
""", conn)

tables
