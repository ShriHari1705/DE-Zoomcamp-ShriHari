variable "credentials" {
  description = "My Credentials"
  default     = "../pipeline/google_credentials.json"
}

variable "project" {
  description = "Project"
  default     = "celtic-vent-487722-f0" # Your actual Project ID
}

variable "region" {
  description = "Region"
  default     = "us-central1"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "trips_data_all"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "celtic-vent-487722-f0-terra-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}