terraform {
  required_version = ">= 0.12"
  backend local {   // gcloud
                                                      // stores Terraform's "state" snapshots, to map real-world resources to your configuration.
                }
  required_providers {
    google = {
      source = "hashicorp/google"

                       // specifies the providers required by the current module
            } 
                      }
}
provider "google" {
    project     = var.project_id
    region      = var.region                    //adds a set of resource types and/or data sources that Terraform can manage
                                                    // The Terraform Registry is the main directory of publicly available providers from most major infrastructure platforms.
}

resource "google_storage_bucket" "data-lake-bucket" {

  name          = "${local.data_lake_bucket}_${var.project_id}"
  location      = var.region
  project       = var.project_id
  force_destroy = true

  # Optional, but recommended settings:
  storage_class = var.storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 30 //
    }
    action {
      type = "Delete"
    }
  }  
  
                                      //blocks to define components of your infrastructure
                                        //Project modules/resources: google_storage_bucket, google_bigquery_dataset, google_bigquery_table
  
}

# DWH
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_dataset
resource "google_bigquery_dataset" "dataset" {
  dataset_id                  = var.BQ_dataset
  project                  = var.project_id
  location                    = var.region
}