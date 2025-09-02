"""
Transformation & Forecast Tests:
Ensure transformed KPI, cohort, and forecast tables are valid and consistent.
"""

import logging
import pandas as pd

log = logging.getLogger("tests.transforms_forecast")


# KPI Table Tests
def test_kpi_daily_not_empty(engine):
    """kpi_daily should have rows"""
    df = pd.read_sql("SELECT COUNT(*) AS cnt FROM kpi_daily", engine)
    log.info(f"kpi_daily row count = {df['cnt'].iloc[0]}")
    assert df["cnt"].iloc[0] > 0, "❌ kpi_daily is empty"


def test_kpi_daily_no_nulls(engine):
    """kpi_daily should not contain NULLs"""
    df = pd.read_sql("""
        SELECT COUNT(*) AS cnt
        FROM kpi_daily
        WHERE order_date IS NULL
           OR total_revenue IS NULL
           OR total_orders IS NULL
           OR unique_customers IS NULL
           OR avg_order_value IS NULL
    """, engine)
    log.info(f"kpi_daily null violations = {df['cnt'].iloc[0]}")
    assert df["cnt"].iloc[0] == 0, "❌ Found NULLs in kpi_daily"


# Cohort Analysis Tests

def test_cohort_analysis_structure(engine):
    """cohort_analysis should have expected columns"""
    df = pd.read_sql("SELECT * FROM cohort_analysis LIMIT 1", engine)
    log.info(f"cohort_analysis columns = {list(df.columns)}")
    required = {"cohort_month", "order_month", "customers", "retained_customers"}
    assert required.issubset(set(df.columns)), "❌ cohort_analysis missing required columns"


def test_cohort_retention_non_negative(engine):
    """Retained customers should never be negative"""
    df = pd.read_sql("SELECT MIN(retained_customers) AS min_val FROM cohort_analysis", engine)
    min_val = df["min_val"].iloc[0]
    log.info(f"Minimum retained_customers = {min_val}")
    assert min_val >= 0, "❌ Found negative retained_customers in cohort_analysis"


# Forecast Table Tests

def test_forecast_has_required_columns(engine):
    """forecast_revenue should have Prophet output columns"""
    df = pd.read_sql("SELECT * FROM forecast_revenue LIMIT 1", engine)
    log.info(f"forecast_revenue columns = {list(df.columns)}")
    required = {"ds", "yhat", "yhat_lower", "yhat_upper"}
    assert required.issubset(set(df.columns)), "❌ forecast_revenue missing required columns"


def test_forecast_length(engine):
    """Forecast should extend at least 1 day beyond history"""
    last_actual = pd.read_sql(
        "SELECT MAX(order_date) AS d FROM kpi_daily", engine
    )["d"].iloc[0]
    last_forecast = pd.read_sql(
        "SELECT MAX(ds) AS d FROM forecast_revenue", engine
    )["d"].iloc[0]

    # Normalize both to Timestamps for safe comparison
    last_actual = pd.to_datetime(last_actual)
    last_forecast = pd.to_datetime(last_forecast)

    log.info(f"Last actual = {last_actual}, Last forecast = {last_forecast}")
    assert last_forecast > last_actual, "❌ Forecast table does not extend beyond history"


def test_forecast_no_nulls(engine):
    """Forecast predictions should not be NULL"""
    df = pd.read_sql("""
        SELECT COUNT(*) AS cnt
        FROM forecast_revenue
        WHERE ds IS NULL OR yhat IS NULL OR yhat_lower IS NULL OR yhat_upper IS NULL
    """, engine)
    log.info(f"forecast_revenue null violations = {df['cnt'].iloc[0]}")
    assert df["cnt"].iloc[0] == 0, "❌ Found NULL values in forecast_revenue"

