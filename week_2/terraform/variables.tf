locals {
  data_lake_bucket = "week_2_datalake"
}

variable "project_id" {
  description = "project_id in google cloud"
  default = "datacamp2"
}

variable "region" {
    description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
    default = "europe-west6"
}

variable "BQ_dataset" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "football"
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}
