# Sales Analytics Pipeline

## Overview
This project implements a production-grade end-to-end data pipeline for sales analytics. The pipeline extracts and cleans sales data, loads it into PostgreSQL, applies business transformations, generates revenue forecasts using Prophet, validates data quality with automated tests, and finally powers interactive Tableau dashboards.

The stack includes:
- **Python** for ETL and forecasting
- **PostgreSQL** as the analytical database
- **Apache Airflow** for orchestration
- **Docker Compose** for containerization
- **Pytest** for validation and quality checks
- **Tableau** for visualization

## Features
- Automated ETL to load raw CSV into PostgreSQL  
- SQL-based transformations to create dimensions, facts, KPIs, and cohort analysis  
- Forecasting with Prophet to project daily revenue  
- Airflow DAG to orchestrate ETL, transform, forecast, validation, and join steps  
- Automated data quality and business rule tests using Pytest  
- Containerized environment with Docker and Docker Compose  
- Tableau dashboards for monitoring sales performance, forecast accuracy, and customer retention  

## Repository Structure
```

├── dags/
│   └── sales\_pipeline\_dag.py
├── etl/
│   └── load\_data.py
├── forecast/
│   └── revenue\_forecast.py
├── db/
│   ├── transform.sql
│   └── join\_actuals\_forecast.sql
├── tests/
│   ├── test\_data\_quality.py
│   ├── test\_business\_quality.py
│   ├── test\_transforms\_and\_forecast.py
│   └── run\_tests.py
├── data/
│   └── sample\_train.csv
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md

````

## Setup Instructions

### 1. Clone Repository
```bash
git clone https://github.com/<your-username>/sales-analytics-pipeline.git
cd sales-analytics-pipeline
````

### 2. Environment Variables

Create a `.env` file in the project root:

```env
DB_USER=salesuser
DB_PASS=salespass
DB_NAME=salesdb
DB_HOST=postgres_db
DB_PORT=5432
CSV_PATH=/opt/airflow/dags/data/train.csv
```

### 3. Build and Start Services

```bash
docker-compose up -d --build airflow-webserver airflow-scheduler
```

Airflow UI will be available at:
**[http://localhost:8080](http://localhost:8080)**
Default login: **admin / admin**

### 4. Run Pipeline

Trigger the DAG named **sales\_pipeline** from the Airflow UI.
The pipeline performs:

* ETL: Load raw sales data into PostgreSQL
* Transform: Create dimensions, facts, KPIs, and cohorts
* Forecast: Generate revenue forecasts with Prophet
* Validation: Execute automated tests
* Join: Combine actuals and forecast for reporting

### 5. Run Tests Locally

```bash
pytest -v tests/
```

### 6. Connect Tableau

1. Configure a PostgreSQL connection in Tableau:

   * Host: `localhost`
   * Port: `5432`
   * Database: `salesdb`
   * User: `salesuser`
   * Password: `salespass`
2. Use tables such as:

   * `kpi_daily`
   * `forecast_revenue`
   * `cohort_analysis`
   * `actual_vs_forecast` (view created by pipeline)
3. Build or open existing dashboards:

   * Performance Trend
   * Total Sales KPI
   * Forecast vs Actuals
   * Customer Retention Analysis

## Dashboards

The following dashboards demonstrate the insights produced by the pipeline:

* Sales Performance and Trends
* Forecast vs Actual Revenue
* Customer Segmentation and Retention

## Requirements

* Docker and Docker Compose
* Tableau Desktop or Tableau Public
* Python 3.8+ (if running locally outside Docker)

Python dependencies are listed in `requirements.txt`.

## Validation

The pipeline includes automated validation checks such as:

* No missing or negative sales values
* Unique order-product combinations
* KPI revenue matches raw sales totals
* Forecasts extend beyond last actual date
* Cohort tables contain required columns

## License


This project is released under the MIT License.
