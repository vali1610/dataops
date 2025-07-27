## 🛠️ Environment Setup

To provision the execution environment, I chose a **cloud-based infrastructure**, specifically **Google Cloud Platform (GCP)**. This approach enables scalable and production-ready orchestration of data pipelines and ML workflows.

Infrastructure provisioning was handled through the GitHub Actions pipeline:  
`.github/workflows/deploy_cloud_infra.yml`

This pipeline executes Terraform scripts and takes the following input parameters:
- `gcp_project_id`: the target GCP project ID  
- `gcp_region`: region for all deployed resources (e.g., `europe-west1`)  
- `gcs_bucket_name`: name of the GCS bucket used for raw/staged data  
- `composer_env_name`: Cloud Composer (Airflow) environment name  
- `dataproc_cluster_name`: Dataproc cluster name (used for Spark jobs)  
- `dataproc_num_workers`: number of worker nodes in the cluster  
- `bq_dataset_name`: BigQuery dataset for metadata & processed data  

### Infrastructure Components

Terraform code is modularized by resource type, located in `terraform/modules/`:
- `bigquery/`: defines the BQ dataset used for downstream data warehousing  
- `composer/`: creates the Cloud Composer 2 environment (managed Airflow)  
- `dataproc/`: provisions the Spark cluster (Apache Spark, Hadoop included)  
- `gcs/`: creates a GCS bucket used for ingesting and staging raw data  

The root `main.tf` file aggregates these modules and is parameterized via `terraform.tfvars`.

After setting up this pipeline, a fully operational data platform is provisioned on GCP, including:
- A **GCS bucket** for data ingestion (where CSV files are stored)  
- A **Dataproc cluster** with preinstalled **Apache Hadoop** and **Apache Spark**  
- A **Cloud Composer 2 environment**, which runs **Apache Airflow DAGs**  
- A **BigQuery dataset** for tracking pipeline metadata and output tables  

> ✅ This setup ensures all Spark, Airflow, and Hadoop requirements mentioned in the challenge are fulfilled via GCP-native services.

## Data Ingestion & Transformation

This section describes how the raw datasets are ingested, processed, and exported in multiple table formats using Apache Spark.

### Dataset

The source dataset used in this project is the [Credit Risk Classification Dataset](https://www.kaggle.com/datasets/praveengovi/credit-risk-classification-dataset) from Kaggle. It contains anonymized features related to credit history and customer behavior, suitable for classification tasks.

---

### Ingestion & Cleaning (Spark on Dataproc)

Ingestion and initial cleaning are performed using the `ingest.py` script located in the `local_processing/` directory.

This script is executed as a **Dataproc job**, and performs the following:

- Reads the raw CSV files (`customer_data_dirty.csv` and `payment_data_dirty.csv`) from a GCS bucket
- Applies cleaning rules:
  - Drops duplicates based on `id`
  - Casts necessary fields to proper data types
  - Filters invalid/missing entries
  - Fills missing values with statistical aggregates (e.g., mean)
- Outputs cleaned Parquet files to a temporary staging path (`/temp`)
- Writes metadata about the step (duration, row counts) as a JSON file in the `/metadata` folder

---

### Data Transformation (Spark on Dataproc)

The `transform.py` script is also run as a **Dataproc job** and performs the following:

- Reads cleaned Parquet files from `/temp`
- Renames and aligns features for consistency
- Converts and writes datasets into multiple formats:
  - ✅ Parquet
  - ✅ CSV (header included)
  - ✅ Delta Lake
  - ✅ Apache Hudi
  - ✅ Apache Iceberg

These outputs are written under the `/output/` folder (e.g., `/output/delta/customer`, `/output/parquet/payment`, etc.).

Corresponding metadata is also saved for tracking and later ingestion.

---

### Verification (Spark on Dataproc)

The `verify.py` script verifies the written outputs across all formats. It is executed as a **Dataproc job**, and for each format:

- Loads and prints schema and sample data
- Counts rows and validates the write
- Supports: CSV, Parquet, Delta, Hudi, Iceberg
- Logs verification results into `verify_metadata.json`

---

### Load to BigQuery (Spark on Dataproc)

The `load_to_bq.py` script loads the transformed Parquet data into BigQuery tables using the `direct` write method.

- Tables: `customer_clean`, `payment_clean`
- Write mode: `WRITE_TRUNCATE`
- Accepts CLI arguments for GCP project ID, dataset, paths
- Logs load operation metadata to `load_metadata.json`

---

### Metadata Logging to BigQuery

The `load_metadata_to_bq.py` script reads all metadata files (`ingest_metadata.json`, `transform_metadata.json`, `verify_metadata.json`, `load_metadata.json`), flattens them, and appends them to a centralized BigQuery table:

- Table: `pipeline_runs` in dataset `dataops_metadata`
- Fields include step name, row counts, format, success status, errors (if any), and durations

This enables full monitoring of the pipeline’s behavior over time.

---

### Deployment & Automation

To make the Spark jobs and dependencies available in GCP:

- The `deploy_pyspark_job.yml` GitHub Actions pipeline:
  - Uploads all Spark job scripts from `local_processing/` to a provisioned GCS bucket
  - Uploads required `.jar` files (for Delta, Hudi, Iceberg support) to the same bucket
  - Syncs the files automatically on any code change via a GitHub trigger

All Dataproc jobs run these `.py` scripts from the GCS bucket.

GCP authentication in all pipelines is handled securely via a service account stored as a GitHub Secret (`GOOGLE_CREDENTIALS_JSON`), used during GitHub Actions workflows.

---

### Technologies Used

- `Apache Spark` for distributed data processing (executed via Dataproc)
- `Delta Lake`, `Apache Hudi`, `Apache Iceberg` for modern data formats
- `Google Cloud Storage` as a data lake
- `BigQuery` for structured storage and metadata logging
- `Terraform` for infrastructure provisioning
- `GitHub Actions` for automated deployment of infrastructure and job artifacts

## Orchestration with Airflow

This section describes how the full ETL flow is orchestrated using Apache Airflow (running inside Google Cloud Composer) and how operational performance and alerts are handled.

### DAG Location

- DAG file: `dags/dataproc-full-dag.py`
- Automatically deployed to Composer via the GitHub Actions pipeline:
  - `.github/workflows/deploy_dags_cloud_composer.yml`
  - Triggered on `push` to `main` branch
  - Uploads all DAGs to the GCS bucket used by the Composer environment

---

### DAG Overview

The DAG is responsible for executing the full end-to-end data pipeline using **Dataproc Spark jobs**. Below are the core steps orchestrated by the DAG:

1. **Ingest CSVs**  
   - Launches `ingest.py` as a Spark job on Dataproc  
   - Cleans and prepares raw data from GCS

2. **Log Ingestion Metrics**  
   - Captures job execution time  
   - Stores structured logs in GCS `/logs/performance_metrics.csv`

3. **Apply Spark Transformations**  
   - Launches `transform.py` as a Spark job on Dataproc  
   - Applies business logic and outputs in 5 formats: CSV, Parquet, Delta, Hudi, Iceberg

4. **Log Transformation Metrics**

5. **Verify Data Integrity**  
   - Launches `verify.py` to read each table format and check schema, row counts, sample values  
   - Ensures no silent errors occurred during writing

6. **Log Verification Metrics**

7. **Load to BigQuery**  
   - Launches `load_to_bq.py` to load cleaned data into BigQuery  
   - Tables: `customer_clean`, `payment_clean`

8. **Log Load Metrics**

9. **Load Metadata to BigQuery**  
   - Executes `load_metadata_to_bq.py`  
   - Centralizes all metadata from previous steps into BigQuery table `dataops_metadata.pipeline_runs`

10. **Check for Alerts**  
    - Executes a shell script (`check_alert.sh`) to scan metadata logs for errors or anomalies  
    - If any alerts are found (e.g., unexpected row drops, schema mismatches), a flag is set

11. **Notify via Slack**  
    - Uses Airflow’s `PythonOperator` to send an alert or success message to a Slack channel  
    - Slack webhook is configured as a Connection in Airflow (`slack_webhook`)

---

### DAG Scheduling

- The DAG is scheduled to run **daily** via `@daily` cron
- Historical backfilling is disabled via `catchup=False`
- DAG is tagged with: `['spark', 'dataproc', 'monitoring']`

---

### Monitoring

- Each step's duration is tracked using `do_xcom_push` and a custom logging function
- Metrics are appended to a CSV log in GCS
- Pipeline success and anomalies are centralized in BigQuery for visual monitoring
- Slack integration ensures real-time visibility in case of pipeline failure or data anomalies

---

### Technologies & Components

| Component        | Usage                                                                 |
|------------------|-----------------------------------------------------------------------|
| **Airflow (Composer)** | DAG definition and orchestration                                  |
| **BashOperator** | Launches `gcloud dataproc jobs submit pyspark` for each script        |
| **PythonOperator** | Used for logging, Slack alerts, and metadata verification logic     |
| **Slack Webhook** | Notifies stakeholders of pipeline outcome                            |
| **XCom**         | Captures execution time for each task                                 |
| **GCS**          | Storage for scripts, input/output data, logs                          |
| **BigQuery**     | Stores pipeline execution metadata                                    |

---

### DAG Diagram

Here is the graph representation of the pipeline orchestrated via Airflow:

<img width="1484" height="571" alt="image" src="https://github.com/user-attachments/assets/1661175f-fd16-4e40-86f7-007bee1e16a5" />

