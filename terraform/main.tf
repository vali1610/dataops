provider "google" {
  credentials = null
  project     = var.project_id
  region      = var.region
}

module "gcs" {
  source      = "./modules/gcs"
  bucket_name = var.bucket_name
  region      = var.region
}

module "composer" {
  source         = "./modules/composer"
  name           = var.composer_name
  region         = var.region
  image_version  = var.composer_image_version
  project_id        = var.project_id
  composer_sa_email = var.composer_sa_email
}

module "dataproc" {
  source      = "./modules/dataproc"
  name        = var.dataproc_name
  region      = var.region
  num_workers = var.dataproc_num_workers
}
