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

variable "spotify_warehouse_datasets" {
  type        = string
  description = "Dataset in BigQuery where raw data (from Google Cloud Storage) will be loaded."
  default     = "spotify_warehouse"
}

variable "spotify_analytics_datasets" {
  type        = string
  description = "Dataset in BigQuery where analytical data will be stored."
  default     = "spotify_analytics"
}


variable "spotify_warehouse_ext_datasets" {
  type        = string
  description = "Dataset in BigQuery where raw data (external tables) will be loaded."
  default     = "spotify_warehouse_ext"
}
