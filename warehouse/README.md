# Data Warehouse

Folder ini berisi implementasi Data Warehouse hasil proses ETL
pada proyek Big Data Final Project.

## 1. Skema Data Warehouse

Model data menggunakan pendekatan **Star Schema** yang terdiri dari:

### Fact Table
- `fact_sales`
  - transaction_key (PK)
  - date_key (FK → dim_date)
  - product_category_id (FK → dim_product_category)
  - trend_id (FK → dim_trend)
  - gross_revenue
  - rev_per_unit
  - trend_for_product

### Dimension Tables
- `dim_date`
- `dim_product_category`
- `dim_trend`

Diagram skema dapat dilihat pada folder `schema/`.

## 2. Proses Load

Data hasil transformasi ETL dimuat ke SQLite database
menggunakan script Python (lihat folder ETL).

File database:
- `coffee_dw.sqlite`

## 3. Data Mart

Selain tabel fakta dan dimensi, disediakan tabel data mart:
- `mart_daily_category_sales`

Tabel ini digunakan untuk kebutuhan dashboard dan analitik agregat harian.

## 4. Query Analitik

Query SQL untuk analisis dan verifikasi tersedia di:
- `sql/analytical_queries.sql`

Query mencakup:
- Total revenue
- Revenue per bulan
- Revenue per kategori
- Weekend vs weekday analysis
- Trend vs revenue analysis

## 5. Verifikasi Integritas Data

Integritas relasi antar tabel diverifikasi melalui query join
dan pengecekan foreign key (lihat `verification_queries.sql`).
