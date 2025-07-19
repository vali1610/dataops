resource "google_composer_environment" "this" {
  name   = var.name
  region = var.region
  config {
    software_config {
      image_version = var.image_version
    }
  }
}
