resource "google_composer_environment" "this" {
  name   = var.name
  region = var.region
  project = var.project_id

  config {
    node_count = 3

    software_config {
      image_version = var.image_version
    }

    workloads_config {
      scheduler {
        cpu        = 1
        memory_gb  = 2
        storage_gb = 1
        count      = 1
      }
    }

    node_config {
      service_account = var.composer_sa_email
    }
  }
}
