"""
Revenue Forecasting Script
- Uses Prophet to generate N-day revenue forecasts
- Refreshes forecast_revenue table (TRUNCATE + reload, so views remain intact)
- Forecast starts strictly after last actual order_date
"""
import os
import logging
from xml.parsers.expat import model
import pandas as pd
from prophet import Prophet
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("forecast.revenue")

# Env variables
load_dotenv()

DB_USER = os.getenv("DB_USER", "salesuser")
DB_PASS = os.getenv("DB_PASS", "salespass")
DB_NAME = os.getenv("DB_NAME", "salesdb")
DB_HOST = os.getenv("DB_HOST", "postgres_db")
DB_PORT = os.getenv("DB_PORT", "5432")

FORECAST_HORIZON = 90  # days

def get_engine():
    return create_engine(
        f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

def main():
    engine = get_engine()

    # 1. Load KPI data
    df = pd.read_sql(
        "SELECT order_date, total_revenue FROM kpi_daily ORDER BY order_date",
        engine
    )

    if df.empty:
        log.error("❌ No data in kpi_daily; aborting forecast")
        return

    # Ensure datetime and clean nulls
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    before = len(df)
    df = df.dropna(subset=["order_date", "total_revenue"])
    dropped = before - len(df)
    if dropped > 0:
        log.warning(f"⚠️ Dropped {dropped} rows with null dates/revenue from kpi_daily")

    if df.empty:
        log.error("❌ No valid data left for forecasting after cleaning")
        return

    last_actual = df["order_date"].max()
    log.info(f"Last actual order_date = {last_actual}")

    prophet_df = df.rename(columns={"order_date": "ds", "total_revenue": "y"})

    # 2. Fit Prophet
    model = Prophet(daily_seasonality=True, yearly_seasonality=True)
    model.fit(prophet_df)

    # 3. Forecast future only
    future = model.make_future_dataframe(
        periods=FORECAST_HORIZON,
        freq="D",
        include_history=True
    )
    forecast = model.predict(future)[["ds", "yhat", "yhat_lower", "yhat_upper"]]

    forecast = forecast[forecast["ds"] > last_actual].reset_index(drop=True)


    # 4. Ensure table exists & refresh it
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS forecast_revenue (
                ds DATE,
                yhat NUMERIC,
                yhat_lower NUMERIC,
                yhat_upper NUMERIC
            );
        """))
        conn.execute(text("TRUNCATE TABLE forecast_revenue;"))

    forecast.to_sql("forecast_revenue", engine, if_exists="append", index=False)

    log.info(f"✅ Forecast table refreshed with {len(forecast)} days")
    log.info(f"Range: {forecast['ds'].min()} → {forecast['ds'].max()}")

if __name__ == "__main__":
    main()

