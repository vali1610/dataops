resource "google_dataproc_cluster" "this" {
  name   = var.name
  region = var.region
  cluster_config {
    master_config {
      num_instances = 1
      machine_type  = "n1-standard-2"
    }
    worker_config {
      num_instances = var.num_workers
      machine_type  = "n1-standard-2"
    }
  }
}
