# Project Phases

## Phase 1: Data Ingestion (GCS)
- Upload transactional CSV data (`food_daily.csv`) to a Google Cloud Storage (GCS) bucket
- Data includes: customer ID, date, time, order ID, items ordered, transaction amount, payment mode, restaurant name, order status, customer ratings, and feedback
- Ensure all files and services are in the same GCS location to avoid read/write location errors

## Phase 2: Data Processing (Apache Beam / Dataflow)
- Develop `beam.py` — an Apache Beam pipeline to process the ingested data
- Parse command-line arguments for input file path
- Clean data: remove trailing colons and special characters
- Split data into two branches: **delivered** and **undelivered** orders
- Compute total record count, delivered count, and undelivered count
- Transform filtered data into JSON format
- Write delivered orders to a BigQuery table
- Write undelivered orders to a separate BigQuery table
- Test the pipeline locally via Cloud Shell before orchestration

## Phase 3: Orchestration (Cloud Composer / Apache Airflow)
- Enable Cloud Composer API and Dataflow API
- Set up a Composer environment (Composer 1 or Composer 2)
- Upload `beam.py` to the Composer GCS bucket
- Configure and deploy the Airflow DAG (`airflow.py` / `airflow2.py`)
- DAG monitors GCS bucket for new files using a sensor
- On file detection: move file to `processed/` subdirectory and delete the original
- Trigger Dataflow job to process the file and load results into BigQuery
- Schedule DAG to run at defined intervals (every 10 minutes or daily)

## Phase 4: Data Warehousing (BigQuery)
- Store processed data in two BigQuery tables:
  - `delivered_orders`
  - `other_status_orders`
- Append new data on each pipeline run
- Delete existing dataset before re-running to avoid duplication during testing

## Phase 5: Reporting (Looker)
- Connect Looker Studio to BigQuery
- Build daily reports using the `delivered_orders` and `other_status_orders` tables
- Visualize key metrics from the food delivery transactional data
