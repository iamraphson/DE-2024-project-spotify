locals {
  DE_2004_PROJECT_DATALAKE = "spotify_project_datalake"
}

variable "project" {
  type        = string
  description = "GCP project ID"
  default     = "radiant-gateway-412001"
}

variable "region" {
  type        = string
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default     = "us-west1"
}

variable "storage_class" {
  type        = string
  description = "The Storage Class of the new bucket. Ref: https://cloud.google.com/storage/docs/storage-classes"
  default     = "STANDARD"
}