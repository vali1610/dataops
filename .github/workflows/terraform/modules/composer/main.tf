resource "google_composer_environment" "this" {
  name   = var.name
  region = var.region
  config {
    node_count = var.node_count
    software_config {
      image_version = var.image_version
    }
  }
}
