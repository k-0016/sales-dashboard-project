"""
Data Quality Tests:
Validate the integrity of raw_sales table before transformations.
"""

import logging
import pandas as pd

# Reuse logging setup from conftest.py
log = logging.getLogger("tests.data_quality")


def test_raw_sales_not_empty(engine):
    """raw_sales table should not be empty"""
    df = pd.read_sql("SELECT COUNT(*) AS cnt FROM raw_sales", engine)
    count = df["cnt"].iloc[0]
    log.info(f"Row count in raw_sales = {count}")
    assert count > 0, "❌ raw_sales is empty"


def test_no_missing_order_ids(engine):
    """Every row should have a non-null Order ID"""
    df = pd.read_sql('SELECT COUNT(*) AS cnt FROM raw_sales WHERE "Order ID" IS NULL', engine)
    missing = df["cnt"].iloc[0]
    log.info(f"Missing Order IDs = {missing}")
    assert missing == 0, f"❌ Found {missing} rows with missing Order IDs"


def test_no_negative_sales(engine):
    """Sales values must be non-negative"""
    df = pd.read_sql('SELECT COUNT(*) AS cnt FROM raw_sales WHERE "Sales" < 0', engine)
    negatives = df["cnt"].iloc[0]
    log.info(f"Negative sales count = {negatives}")
    assert negatives == 0, f"❌ Found {negatives} rows with negative Sales"


def test_valid_dates(engine):
    """Order Date and Ship Date must not be null"""
    df = pd.read_sql('''
        SELECT COUNT(*) AS cnt
        FROM raw_sales
        WHERE "Order Date" IS NULL OR "Ship Date" IS NULL
    ''', engine)
    invalid = df["cnt"].iloc[0]
    log.info(f"Invalid dates count = {invalid}")
    assert invalid == 0, f"❌ Found {invalid} rows with null Order/Ship dates"


def test_unique_order_product_combo(engine):
    """Each Order ID + Product ID pair should be unique"""
    df = pd.read_sql('''
        SELECT "Order ID", "Product ID", COUNT(*) AS cnt
        FROM raw_sales
        GROUP BY "Order ID", "Product ID"
        HAVING COUNT(*) > 1
    ''', engine)
    dupes = len(df)
    log.info(f"Duplicate Order/Product combos = {dupes}")
    assert df.empty, f"❌ Found {dupes} duplicate Order/Product combinations"
