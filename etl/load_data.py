"""
ETL Script: Clean and load raw sales CSV into PostgreSQL (EDA-driven rules)
- Auto-detects CSV path (local vs Airflow container)
- Parses dates with dayfirst=True
- Drops rows with missing critical fields (Order ID, Product ID, Order Date, Ship Date, Sales)
- Aggregates duplicates on (Order ID, Product ID) by summing Sales
- Validates non-negative Sales
- Logs every step; idempotent load (replace table each run)
"""
import os
import sys
import logging
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# -------------------------------
# Logging config
# -------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger("etl.load_data")

# -------------------------------
# Load environment variables
# -------------------------------
load_dotenv()

DB_USER = os.getenv("DB_USER", "salesuser")
DB_PASS = os.getenv("DB_PASS", "salespass")
DB_NAME = os.getenv("DB_NAME", "salesdb")
DB_HOST = os.getenv("DB_HOST", "postgres_db")  # For Docker networking
DB_PORT = os.getenv("DB_PORT", "5432")

TABLE_NAME = "raw_sales"
CHUNKSIZE = 1000

# -------------------------------
# CSV Path Detection
# -------------------------------
CSV_PATH = os.getenv("CSV_PATH")
if not CSV_PATH:
    if os.path.exists("data/train.csv"):
        CSV_PATH = "data/train.csv"  # Local dev mode
    else:
        CSV_PATH = "/opt/airflow/dags/data/train.csv"  # Airflow container mode


def get_engine():
    """Create a SQLAlchemy engine for Postgres"""
    return create_engine(
        f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )


def main():
    """Main ETL process"""
    # -------------------------------
    # 1) Load CSV
    # -------------------------------
    try:
        df = pd.read_csv(CSV_PATH)
        log.info(f"✅ Loaded CSV: rows={len(df):,}, cols={len(df.columns)} from {CSV_PATH}")
    except Exception as e:
        log.error(f"❌ Failed to read CSV at {CSV_PATH}: {e}")
        raise

    # Required columns
    required = [
        "Order ID", "Product ID", "Order Date", "Ship Date",
        "Customer ID", "Sales"
    ]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"❌ Missing required column: {col}")

    # -------------------------------
    # 2) Parse dates
    # -------------------------------
    for col in ["Order Date", "Ship Date"]:
        df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)

    # -------------------------------
    # 3) Coerce numeric fields
    # -------------------------------
    df["Sales"] = pd.to_numeric(df["Sales"], errors="coerce")

    # -------------------------------
    # 4) Drop rows with missing critical fields
    # -------------------------------
    before = len(df)
    df = df.dropna(subset=required)
    dropped = before - len(df)
    log.info(f"🧹 Dropped {dropped:,} rows with NULLs in critical fields")

    # -------------------------------
    # 5) Validate non-negative sales
    # -------------------------------
    neg_ct = (df["Sales"] < 0).sum()
    if neg_ct > 0:
        raise ValueError(f"❌ Found {neg_ct:,} rows with negative Sales")

    # -------------------------------
    # 6) Aggregate duplicates
    # -------------------------------
    before = len(df)
    agg_fields = {
        "Order Date": "first",
        "Ship Date": "first",
        "Ship Mode": "first",
        "Customer ID": "first",
        "Customer Name": "first",
        "Segment": "first",
        "Country": "first",
        "City": "first",
        "State": "first",
        "Postal Code": "first",
        "Region": "first",
        "Category": "first",
        "Sub-Category": "first",
        "Product Name": "first",
        "Sales": "sum",
    }
    present_agg = {k: v for k, v in agg_fields.items() if k in df.columns}

    df = df.groupby(["Order ID", "Product ID"], as_index=False).agg(present_agg)
    after = len(df)
    log.info(f"🔄 Aggregated {before - after:,} duplicate Order/Product rows")

    # -------------------------------
    # 7) Final type fix for Postal Code
    # -------------------------------
    if "Postal Code" in df.columns:
        try:
            df["Postal Code"] = pd.to_numeric(df["Postal Code"], errors="coerce").astype("Int64")
        except Exception as e:
            log.warning(f"⚠️ Could not cast 'Postal Code' to Int64: {e}")

    # -------------------------------
    # 8) Final validations
    # -------------------------------
    if df.empty:
        raise ValueError("❌ No rows left after cleaning; aborting load")

    if df["Order Date"].isna().any() or df["Ship Date"].isna().any():
        raise ValueError("❌ Found NULL dates after cleaning; aborting load")

    # -------------------------------
    # 9) Load to Postgres
    # -------------------------------
    engine = get_engine()
    try:
        df.to_sql(
            TABLE_NAME,
            engine,
            if_exists="replace",  # idempotent
            index=False,
            method="multi",
            chunksize=CHUNKSIZE
        )
        log.info(f"🎉 Loaded table '{TABLE_NAME}' with {len(df):,} rows")
    except Exception as e:
        log.error(f"❌ Insert into Postgres failed: {e}")
        raise


if __name__ == "__main__":
    main()
