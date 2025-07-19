variable "credentials_file" { type = string }
variable "project_id"        { type = string }
variable "region"            { type = string }

variable "bucket_name"       { type = string }

variable "composer_name"          { type = string }
variable "composer_image_version" { type = string }

variable "dataproc_name"        { type = string }
variable "dataproc_num_workers" { type = number }
variable "composer_sa_email" {
  description = "Email of the service account used for Composer"
  type        = string
}