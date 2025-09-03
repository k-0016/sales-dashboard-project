## Setup  

git clone https://github.com/<your-username>/sales-analytics-pipeline.git
cd sales-analytics-pipeline
2. Configure Environment Variables
Create a .env file in the project root:

bash
Copy code
DB_USER=salesuser
DB_PASS=salespass
DB_NAME=salesdb
DB_HOST=postgres_db
DB_PORT=5432
CSV_PATH=/opt/airflow/dags/data/train.csv
3. Start Services
bash
Copy code
docker-compose up -d --build airflow-webserver airflow-scheduler
Airflow UI will be available at: http://localhost:8080
Login credentials: admin / admin

4. Run Pipeline
Trigger the sales_pipeline DAG in Airflow.
Pipeline stages:

ETL → Load raw sales data into PostgreSQL

Transform → Create dimensions, facts, KPIs, and cohort tables

Forecast → Generate revenue forecasts using Prophet

Validate → Execute automated data quality checks

Join → Combine actuals and forecasts for reporting

5. Run Tests Locally
bash
Copy code
pytest -v tests/
6. Connect Tableau
Connect to PostgreSQL and use tables or views such as:

kpi_daily

forecast_revenue

cohort_analysis

actual_vs_forecast

How to Run
Make sure you have Docker and Docker Compose installed.

Clone the repository and set up the .env file.

Start all services using:

bash
Copy code
docker-compose up -d --build
Access the Airflow UI at http://localhost:8080 and trigger the sales_pipeline DAG.

Once the pipeline completes, connect Tableau to PostgreSQL to visualize KPIs, forecasts, and cohort analytics.


