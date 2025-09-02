-- ==========================================
-- DROP old objects if they exist
-- ==========================================
DROP TABLE IF EXISTS dim_customer CASCADE;
DROP TABLE IF EXISTS dim_product CASCADE;
DROP TABLE IF EXISTS fact_sales CASCADE;
DROP TABLE IF EXISTS kpi_daily CASCADE;
DROP TABLE IF EXISTS cohort_analysis CASCADE;

-- ==========================================
-- 1. Dimension: Customer
-- ==========================================
CREATE TABLE dim_customer AS
SELECT DISTINCT
    "Customer ID"   AS customer_id,
    "Customer Name" AS customer_name,
    "Segment"       AS segment,
    "Country"       AS country,
    "City"          AS city,
    "State"         AS state,
    "Postal Code"   AS postal_code,
    "Region"        AS region
FROM raw_sales;

-- ==========================================
-- 2. Dimension: Product
-- ==========================================
CREATE TABLE dim_product AS
SELECT DISTINCT
    "Product ID"    AS product_id,
    "Category"      AS category,
    "Sub-Category"  AS sub_category,
    "Product Name"  AS product_name
FROM raw_sales;

-- ==========================================
-- 3. Fact: Sales
-- Deduplicate by Order ID + Product ID
-- ==========================================
CREATE TABLE fact_sales AS
SELECT
    ROW_NUMBER() OVER ()         AS row_id,
    "Order ID"                   AS order_id,
    "Product ID"                 AS product_id,
    MIN("Order Date")::DATE      AS order_date,
    MIN("Ship Date")::DATE       AS ship_date,
    MIN("Ship Mode")             AS ship_mode,
    MIN("Customer ID")           AS customer_id,
    SUM("Sales")::NUMERIC        AS sales
FROM raw_sales
GROUP BY "Order ID", "Product ID";

-- ==========================================
-- 4. KPI Table (Daily Metrics)
-- avg_order_value guarded against nulls
-- ==========================================
CREATE TABLE kpi_daily AS
SELECT
    order_date,
    COUNT(DISTINCT order_id)    AS total_orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(sales)                  AS total_revenue,
    CASE
        WHEN COUNT(DISTINCT order_id) = 0 THEN 0
        ELSE SUM(sales)::NUMERIC / COUNT(DISTINCT order_id)
    END AS avg_order_value
FROM fact_sales
GROUP BY order_date
ORDER BY order_date;

-- ==========================================
-- 5. Cohort Analysis (Retention)
-- Proper CREATE TABLE with CTE
-- ==========================================
CREATE TABLE cohort_analysis AS
WITH first_purchase AS (
    SELECT
        customer_id,
        DATE_TRUNC('month', MIN(order_date))::DATE AS cohort_month
    FROM fact_sales
    GROUP BY customer_id
),
order_periods AS (
    SELECT
        f.customer_id,
        DATE_TRUNC('month', f.order_date)::DATE AS order_month,
        fp.cohort_month
    FROM fact_sales f
    JOIN first_purchase fp ON f.customer_id = fp.customer_id
)
SELECT
    cohort_month,
    order_month,
    COUNT(DISTINCT customer_id) AS customers,
    COUNT(DISTINCT CASE WHEN order_month > cohort_month THEN customer_id END) AS retained_customers
FROM order_periods
GROUP BY cohort_month, order_month
ORDER BY cohort_month, order_month;
