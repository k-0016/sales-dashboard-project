    -- Drop the view if it already exists to ensure a clean update.
    DROP VIEW IF EXISTS actual_vs_forecast;

    -- Create a single, clean view for Tableau to consume for time-series analysis.
    -- This view combines historical actual revenue with future forecasted revenue.
    CREATE VIEW actual_vs_forecast AS
    SELECT
        -- COALESCE ensures a single continuous timeline.
        COALESCE(k.order_date::DATE, f.ds::DATE) AS ds,
        k.total_revenue AS actual_revenue,
        f.yhat AS forecast,
        f.yhat_lower,
        f.yhat_upper
    FROM kpi_daily k
    FULL OUTER JOIN forecast_revenue f
        ON k.order_date::DATE = f.ds::DATE
    ORDER BY ds;
