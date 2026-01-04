USE fleximart_dw;

-- ============================================================
-- Query 1: Monthly Sales Drill-Down Analysis
-- Scenario: "Yearly total, then quarterly, then monthly sales for 2024"
-- Demonstrates: Drill-down Year → Quarter → Month
-- Expected: year | quarter | month_name | total_sales | total_quantity
-- ============================================================

SELECT
  d.year,
  d.quarter,
  d.month_name,
  ROUND(SUM(f.total_amount), 2) AS total_sales,
  SUM(f.quantity_sold) AS total_quantity
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
WHERE d.year = 2024
GROUP BY d.year, d.quarter, d.month, d.month_name
ORDER BY d.year, d.quarter, d.month;


-- ============================================================
-- Query 2: Product Performance Analysis
-- Scenario: "Top 10 products by revenue + category + units + revenue %"
-- Expected: product_name | category | units_sold | revenue | revenue_percentage
-- ============================================================

SELECT
  p.product_name,
  p.category,
  SUM(f.quantity_sold) AS units_sold,
  ROUND(SUM(f.total_amount), 2) AS revenue,
  ROUND(
    100 * SUM(f.total_amount) / SUM(SUM(f.total_amount)) OVER (),
    2
  ) AS revenue_percentage
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.product_key, p.product_name, p.category
ORDER BY revenue DESC
LIMIT 10;


-- ============================================================
-- Query 3: Customer Segmentation Analysis
-- Scenario: "Segment customers by total spend (High/Medium/Low)"
-- High Value: > 50000
-- Medium Value: 20000–50000
-- Low Value: < 20000
-- Output: segment | customer_count | total_revenue | avg_revenue_per_customer
-- ============================================================

WITH customer_spend AS (
  SELECT
    c.customer_key,
    c.customer_name,
    SUM(f.total_amount) AS total_spent
  FROM fact_sales f
  JOIN dim_customer c ON f.customer_key = c.customer_key
  GROUP BY c.customer_key, c.customer_name
),
segmented AS (
  SELECT
    CASE
      WHEN total_spent > 50000 THEN 'High Value'
      WHEN total_spent >= 20000 THEN 'Medium Value'
      ELSE 'Low Value'
    END AS customer_segment,
    total_spent
  FROM customer_spend
)
SELECT
  customer_segment,
  COUNT(*) AS customer_count,
  ROUND(SUM(total_spent), 2) AS total_revenue,
  ROUND(AVG(total_spent), 2) AS avg_revenue_per_customer
FROM segmented
GROUP BY customer_segment
ORDER BY total_revenue DESC;
