"""
Business Quality Tests:
Validate that transformed dimension, KPI, and forecast tables
make business sense (not just data integrity).
"""

import logging
import pandas as pd

log = logging.getLogger("tests.business_quality")


# -------------------------------
# Dimension Tests
# -------------------------------

def test_segment_values(engine):
    """Segments in dim_customer should only be expected categories"""
    df = pd.read_sql("SELECT DISTINCT segment FROM dim_customer", engine)
    allowed = {"Consumer", "Corporate", "Home Office"}
    bad = set(df["segment"]) - allowed
    log.info(f"Segments found = {set(df['segment'])}")
    assert not bad, f"❌ Unexpected segment values found: {bad}"


def test_region_values(engine):
    """Regions in dim_customer should only be expected categories"""
    df = pd.read_sql("SELECT DISTINCT region FROM dim_customer", engine)
    allowed = {"West", "East", "Central", "South"}
    bad = set(df["region"]) - allowed
    log.info(f"Regions found = {set(df['region'])}")
    assert not bad, f"❌ Unexpected region values found: {bad}"


# -------------------------------
# KPI vs Raw Consistency
# -------------------------------

def test_fact_vs_raw_revenue(engine):
    """Fact/KPI revenue should exactly match raw sales totals"""
    raw_df = pd.read_sql('SELECT SUM("Sales")::NUMERIC AS raw_sum FROM raw_sales', engine)
    fact_df = pd.read_sql('SELECT SUM(total_revenue)::NUMERIC AS fact_sum FROM kpi_daily', engine)

    raw_sum = raw_df["raw_sum"].iloc[0]
    fact_sum = fact_df["fact_sum"].iloc[0]
    diff = abs(raw_sum - fact_sum)

    log.info(f"Raw total={raw_sum}, Fact total={fact_sum}, Diff={diff}")
    assert diff < 1e-6, f"❌ Fact revenue does not match raw sales (diff={diff})"


# -------------------------------
# Forecast Tests
# -------------------------------

def test_forecast_continuity(engine):
    """Forecast should start after the last actual KPI date"""
    last_actual = pd.read_sql(
        "SELECT MAX(order_date)::DATE AS last_actual FROM kpi_daily", engine
    )["last_actual"].iloc[0]

    first_forecast = pd.read_sql(
        "SELECT MIN(ds)::DATE AS first_forecast FROM forecast_revenue", engine
    )["first_forecast"].iloc[0]

    log.info(f"Last actual={last_actual}, First forecast={first_forecast}")
    assert first_forecast > last_actual, (
        f"❌ Forecast starts before last actual date "
        f"(last={last_actual}, first={first_forecast})"
    )


def test_forecast_ranges(engine):
    """Forecast values must be non-negative and within prediction intervals"""
    df = pd.read_sql("SELECT * FROM forecast_revenue", engine)

    bad_bounds = df[(df["yhat"] < df["yhat_lower"]) | (df["yhat"] > df["yhat_upper"])]
    negatives = df[df["yhat"] < 0]

    log.info(f"Checked {len(df)} forecast rows")
    assert bad_bounds.empty, "❌ Some forecasts fall outside prediction intervals"
    assert negatives.empty, "❌ Some forecasts are negative"
