# Project Tasks

## GCS Tasks
- [ ] Create a GCS bucket in the desired region
- [ ] Upload `food_daily.csv` to the GCS bucket
- [ ] Ensure bucket region matches BigQuery dataset region to avoid location errors

## Beam / Dataflow Tasks
- [ ] Set GCP project in Cloud Shell: `gcloud config set project your-project-id`
- [ ] Install Apache Beam: `pip install apache-beam[gcp]`
- [ ] Write `beam.py` pipeline with the following logic:
  - [ ] Parse `--input` and `--temp_location` arguments
  - [ ] Read and clean input CSV data
  - [ ] Split data into delivered and undelivered branches
  - [ ] Count total, delivered, and undelivered records
  - [ ] Write delivered orders to BigQuery table
  - [ ] Write undelivered orders to BigQuery table
- [ ] Test pipeline locally: `python beam.py --input gs://your-bucket/food_daily.csv --temp_location gs://your-bucket`
- [ ] Verify results in BigQuery after test run
- [ ] Delete BigQuery dataset before re-running to avoid duplicate appends

## Composer / Airflow Tasks
- [ ] Enable APIs: `gcloud services enable composer.googleapis.com dataflow.googleapis.com`
- [ ] Create a Composer environment (Composer 1 or Composer 2)
- [ ] Upload `beam.py` to the Composer GCS bucket
- [ ] Copy `gsutil URI` of `beam.py` and update `py_file` in the DAG
- [ ] Write Airflow DAG with the following tasks:
  - [ ] `gcs_sensor` — monitor GCS bucket for new files with prefix `food_daily`
  - [ ] `list_files` — move detected file to `processed/` folder and delete original
  - [ ] `beamtask` — trigger Dataflow job using the processed file as input
- [ ] Upload DAG file (`airflow.py` for Composer 1 / `airflow2.py` for Composer 2) to the dags folder
- [ ] Verify DAG appears in Airflow UI
- [ ] Trigger DAG manually or wait for scheduled run
- [ ] Review logs for each task: `gcs_sensor`, `list_files`, `beamtask`
- [ ] Confirm Dataflow job completes successfully
- [ ] Verify data loaded into BigQuery tables
- [ ] Test with a new timestamped file to confirm append behavior
- [ ] Delete Composer environment after use to avoid unnecessary costs

## BigQuery Tasks
- [ ] Confirm two tables are created: `delivered_orders` and `other_status_orders`
- [ ] Query tables to validate data correctness
- [ ] Check XComs in Airflow UI for task output values

## Looker Tasks
- [ ] Connect Looker Studio to BigQuery: https://lookerstudio.google.com
- [ ] Select BigQuery as the data source
- [ ] Build daily report using `delivered_orders` and `other_status_orders` tables
- [ ] Publish and share the report
