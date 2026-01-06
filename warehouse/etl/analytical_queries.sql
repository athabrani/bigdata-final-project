-- Q1 Total Revenue
SELECT ROUND(SUM(gross_revenue), 2) AS total_revenue
FROM fact_sales;

-- Q2 Revenue by Month
SELECT d.year, d.month,
       COUNT(f.transaction_key) AS n_transactions,
       ROUND(SUM(f.gross_revenue), 2) AS revenue
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- Q3 Revenue by Category
SELECT p.product_category_mapped,
       COUNT(f.transaction_key) AS n_transactions,
       ROUND(SUM(f.gross_revenue), 2) AS revenue
FROM fact_sales f
JOIN dim_product_category p
ON f.product_category_id = p.product_category_id
GROUP BY p.product_category_mapped;
