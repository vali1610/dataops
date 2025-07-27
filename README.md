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
