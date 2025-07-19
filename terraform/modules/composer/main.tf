resource "google_composer_environment" "this" {
  name   = var.name
  region = var.region
  project = var.project_id

  config {
    node_count = 1

    software_config {
      image_version = var.image_version
    }

    environment_size = "ENVIRONMENT_SIZE_SMALL"

    workloads_config {
      scheduler {
        cpu        = 1
        memory_gb  = 2
        storage_gb = 1
        count      = 1
      }
    }

    service_account = var.composer_sa_email
  }
}
