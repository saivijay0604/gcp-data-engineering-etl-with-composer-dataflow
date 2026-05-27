# Project Phases

## Phase 1: Data Ingestion (GCS)

### Step 1: Create your project folder
- Create a local folder `food-gcs-ingestion/` containing:
  - `food_daily.csv`
  - `upload_to_gcs.py`
  - `service-account-key.json`

### Step 2: Create or keep your CSV file
- Create `food_daily.csv` with columns: customer_id, date, time, order_id, items_ordered, transaction_amount, payment_mode, restaurant_name, order_status, customer_ratings, feedback
- Save it in the same folder as your Python script

### Step 3: Create a GCS bucket
- Go to Google Cloud Console → Cloud Storage → Buckets → Create
- Enter a globally unique bucket name e.g. `food-transaction-data-yourname`
- Location type: `Region`
- Location: `us-central1` — use the same location for all GCP services to avoid read/write location mismatch errors
- Storage class: `Standard`
- Access control: `Uniform`
- Click **Create**

### Step 4: Create a service account key
- Go to IAM & Admin → Service Accounts → Create Service Account
- Name: `gcs-upload-service`
- Role: `Storage Object Admin`
- Go to Keys → Add Key → Create new key → JSON → Download
- Rename to `service-account-key.json` and place in project folder

### Step 5: Install Python package
```bash
pip install google-cloud-storage
```

### Step 6: Write the Python script
- Create `upload_to_gcs.py` using the `google-cloud-storage` library
- Set `BUCKET_NAME`, `LOCAL_FILE_PATH`, `GCS_FOLDER`, `DESTINATION_FILE_NAME`
- Authenticate using `os.environ["GOOGLE_APPLICATION_CREDENTIALS"]`
- Upload using `blob.upload_from_filename(local_file_path)`

### Step 7: Run the script
```bash
python upload_to_gcs.py
```
- Expected output: `Upload successful!` with local and GCS path printed

### Step 8: Verify file in GCS
- Go to Cloud Storage → your bucket → `raw/transactions/` → confirm `food_daily.csv` is present

### Step 9: Final GCS folder structure
```text
gs://food-transaction-data-yourname/
└── raw/
    └── transactions/
        └── food_daily.csv
```

### Checklist
- [ ] Create project folder
- [ ] Create food_daily.csv
- [ ] Create GCS bucket in us-central1
- [ ] Create service account key
- [ ] Install google-cloud-storage
- [ ] Write upload_to_gcs.py
- [ ] Run Python script
- [ ] Verify file in GCS bucket

## Phase 2: Data Processing (Apache Beam / Dataflow)

### Step 1: Write beam.py pipeline
- ✅ Develop `beam.py` — an Apache Beam pipeline to process the ingested data
- ✅ Parse command-line arguments for `--input` and `--temp_location`
- ✅ Clean data: remove trailing colons and special characters
- ✅ Split data into two branches: **delivered** and **undelivered** orders
- ✅ Compute total record count, delivered count, and undelivered count
- ✅ Transform filtered data into JSON format
- ✅ Write delivered orders to BigQuery table `delivered_orders`
- ✅ Write undelivered orders to BigQuery table `other_status_orders`

### Step 2: Install Apache Beam
- ✅ Apache Beam installed at `C:\Users\prane\AppData\Local\Programs\Python\Python312`
```bash
pip install apache-beam[gcp]
```

### Step 3: Authenticate and set project in Cloud Shell
- ❌ Login to GCP
```bash
gcloud auth login
```
- ❌ Set project
```bash
gcloud config set project food-delivery-etl-497019
```

### Step 4: Create BigQuery dataset
- ❌ Create dataset `food_orders` in BigQuery
```bash
bq mk --dataset food-delivery-etl-497019:food_orders
```

### Step 5: Update beam.py with actual values
- ❌ Replace placeholder table names in `beam.py`:
  - `food-delivery-etl-497019:food_orders.delivered_orders`
  - `food-delivery-etl-497019:food_orders.other_status_orders`

### Step 6: Test pipeline via Cloud Shell
- ❌ Upload `beam.py` to Cloud Shell and run:
```bash
python beam.py --input gs://foodservice_bucket/food_daily.csv --temp_location gs://foodservice_bucket/temp
```

### Step 7: Verify results in BigQuery
- ❌ Go to BigQuery → `food-delivery-etl-497019` → `food_orders`
- ❌ Confirm two tables created: `delivered_orders` and `other_status_orders`
- ❌ Query tables to validate data

### Checklist
- [x] Write beam.py
- [x] Install apache-beam
- [ ] Authenticate with gcloud auth login
- [ ] Set GCP project
- [ ] Create BigQuery dataset
- [ ] Update beam.py table names
- [ ] Test pipeline in Cloud Shell
- [ ] Verify results in BigQuery

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
