resource "google_bigquery_dataset" "this" {
  dataset_id = var.bq_dataset_name
  project    = var.project_id
  location   = var.location
}