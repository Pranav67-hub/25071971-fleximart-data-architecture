-- Database: fleximart
USE fleximart;

-- ============================================================
-- Query 1: Customer Purchase History
-- Business Question: "Generate a detailed report showing each customer's name, email,
-- total number of orders placed, and total amount spent. Include only customers who have
-- placed at least 2 orders and spent more than ₹5,000. Order by total amount spent DESC."
-- Requirements: Must join customers, orders, order_items. GROUP BY + HAVING. Aggregates.
-- Expected columns: customer_name | email | total_orders | total_spent
-- ============================================================

SELECT
  CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
  c.email,
  COUNT(DISTINCT o.order_id) AS total_orders,
  ROUND(SUM(oi.subtotal), 2) AS total_spent
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY c.customer_id, c.email, c.first_name, c.last_name
HAVING COUNT(DISTINCT o.order_id) >= 2
   AND SUM(oi.subtotal) > 5000
ORDER BY total_spent DESC;


-- ============================================================
-- Query 2: Product Sales Analysis
-- Business Question: "For each product category, show category name, number of different
-- products sold, total quantity sold, and total revenue. Only include categories with
-- revenue > ₹10,000. Order by revenue DESC."
-- Requirements: Join products, order_items. GROUP BY + HAVING. COUNT(DISTINCT), SUM.
-- Expected columns: category | num_products | total_quantity_sold | total_revenue
-- ============================================================

SELECT
  p.category,
  COUNT(DISTINCT p.product_id) AS num_products,
  SUM(oi.quantity) AS total_quantity_sold,
  ROUND(SUM(oi.subtotal), 2) AS total_revenue
FROM products p
JOIN order_items oi ON p.product_id = oi.product_id
GROUP BY p.category
HAVING SUM(oi.subtotal) > 10000
ORDER BY total_revenue DESC;


-- ============================================================
-- Query 3: Monthly Sales Trend (Year 2024)
-- Business Question: "Show monthly sales trends for 2024: month name, total orders,
-- monthly revenue, cumulative revenue (running total Jan..month)."
-- Requirements: Window function SUM() OVER. Month extraction. Chronological order.
-- Expected columns: month_name | total_orders | monthly_revenue | cumulative_revenue
-- ============================================================

WITH monthly AS (
  SELECT
    MONTH(o.order_date) AS month_num,
    MONTHNAME(o.order_date) AS month_name,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(oi.subtotal) AS monthly_revenue
  FROM orders o
  JOIN order_items oi ON o.order_id = oi.order_id
  WHERE YEAR(o.order_date) = 2024
  GROUP BY MONTH(o.order_date), MONTHNAME(o.order_date)
)
SELECT
  month_name,
  total_orders,
  ROUND(monthly_revenue, 2) AS monthly_revenue,
  ROUND(SUM(monthly_revenue) OVER (ORDER BY month_num), 2) AS cumulative_revenue
FROM monthly
ORDER BY month_num;
