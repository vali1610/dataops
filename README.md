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

The source dataset is based on the [Home Credit Default Risk dataset](https://www.kaggle.com/competitions/home-credit-default-risk), downloaded in CSV format. It contains anonymized customer and credit data used for predictive modeling.

---

### Ingestion & Cleaning (Spark)

Ingestion and cleaning are handled using the `ingest.py` script located under `local_processing/`.

This script:

- Reads two raw CSVs (`customer_data_dirty.csv` and `payment_data_dirty.csv`)
- Cleans and filters invalid or missing entries
- Casts types and fills missing values (e.g., mean imputation for some features)
- Drops duplicates based on `id`
- Stores the cleaned data as Parquet files in a temporary staging path

Metadata such as row count, processing time, and status is logged into a JSON file and can be further pushed to BigQuery.

---

### Data Transformation (Spark)

The transformation is implemented in `transform.py` and includes:

- Renaming and aligning features between datasets
- Writing cleaned data into multiple formats:
  - ✅ Parquet
  - ✅ CSV (with header)
  - ✅ Delta Lake (`.format("delta")`)
  - ✅ Apache Hudi (insert operation, partitioned by id)
  - ✅ Apache Iceberg (using Spark SQL catalog and Hadoop warehouse)

Each write operation produces structured outputs in corresponding subfolders (e.g., `/output/delta/customer`).

Metadata for each step is also generated and saved as JSON (duration, rows, etc.).

---

### Verification (Spark)

The `verify.py` script runs after the transformations to validate the written data formats:

- Tries to load and display schema + top 5 records for each format
- Logs row count and load status
- Supports: CSV, Parquet, Delta, Hudi, and Iceberg (as Spark table via `gcs.db.customer`)
- Writes a `verify_metadata.json` file with all verification results

---

### Load to BigQuery

The `load_to_bq.py` script:

- Reads cleaned Parquet files
- Loads them into BigQuery using the `direct` method with `WRITE_TRUNCATE`
- Supports CLI args for `project_id`, `dataset`, `customer_path`, `payment_path`
- Saves load summary and metadata (row count, duration) if output base path is provided

---

### Metadata Logging to BigQuery

The `load_metadata_to_bq.py` script:

- Parses all JSON metadata files (ingest, transform, verify, load)
- Flattens nested JSON fields into BigQuery-compatible schema
- Appends all logs into a central BigQuery table (`dataops_metadata.pipeline_runs`)
- Helps track and visualize pipeline execution over time

---

### Deployment & Automation

To automate and sync the local Spark scripts and dependencies (e.g., `.py` and `.jar` files) to the cloud:

- The `deploy_pyspark_job.yml` GitHub Actions pipeline copies:
  - All Spark scripts from `local_processing/` to a GCS bucket
  - Dependencies for Delta, Hudi, Iceberg to the same bucket
- A trigger ensures this pipeline runs on any change in relevant job files
- Authentication is handled securely via a GCP service account stored in GitHub Secrets

This ensures reproducibility and centralized storage of Spark jobs in the cloud.

---

### Technologies Used

- `Apache Spark` for distributed processing
- `Delta Lake`, `Apache Hudi`, `Apache Iceberg` for modern data lake formats
- `Google Cloud Storage` as data lake storage backend
- `BigQuery` for structured storage of both datasets and metadata
- `GitHub Actions` for automation
- `Terraform` for provisioning all infrastructure components
