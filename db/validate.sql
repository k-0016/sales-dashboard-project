-- ==========================================
-- Validation Script: Raw vs Fact vs KPI vs Forecast vs Cohort
-- Run after ETL + Transform + Forecast
-- ==========================================

-- 1. Row counts
SELECT 'raw_sales' AS table, COUNT(*) AS rows FROM raw_sales
UNION ALL
SELECT 'fact_sales', COUNT(*) FROM fact_sales
UNION ALL
SELECT 'kpi_daily', COUNT(*) FROM kpi_daily
UNION ALL
SELECT 'forecast_revenue', COUNT(*) FROM forecast_revenue
UNION ALL
SELECT 'cohort_analysis', COUNT(*) FROM cohort_analysis;

-- 2. Revenue totals
SELECT 'raw_sales' AS source, SUM("Sales")::NUMERIC AS total FROM raw_sales
UNION ALL
SELECT 'fact_sales', SUM(sales)::NUMERIC FROM fact_sales
UNION ALL
SELECT 'kpi_daily', SUM(total_revenue)::NUMERIC FROM kpi_daily;

-- 3. KPI daily NULL check
SELECT COUNT(*) AS null_violations
FROM kpi_daily
WHERE order_date IS NULL
   OR total_orders IS NULL
   OR unique_customers IS NULL
   OR total_revenue IS NULL
   OR avg_order_value IS NULL;

-- 4. Forecast range
SELECT MIN(ds) AS forecast_start, MAX(ds) AS forecast_end FROM forecast_revenue;

-- 5. Cohort analysis schema check
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'cohort_analysis'
ORDER BY ordinal_position;
