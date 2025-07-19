variable "name"          { type = string }
variable "region"        { type = string }
variable "image_version" { type = string }
variable "project_id"    { type = string }
variable "composer_sa_email" {
  description = "Email of the service account used for Composer"
  type        = string
}