provider "google" {
  project = var.project_id
  region  = var.region
}

variable "project_id" {
  description = "The ID of the GCP project"
  type        = string
  default     = "aero-project-2025"
}

variable "region" {
  description = "The region to deploy resources"
  type        = string
  default     = "asia-southeast1"
}

variable "bucket_name" {
  description = "The name of the GCS bucket"
  type        = string
  default     = "aero-data-lake-2025"
}

# 1. Google Cloud Storage Bucket
resource "google_storage_bucket" "data_lake" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true
}

# 2. BigQuery Dataset
resource "google_bigquery_dataset" "warehouse" {
  dataset_id                  = "flight_data_warehouse"
  friendly_name               = "Flight Data Warehouse"
  description                 = "Data warehouse for AERO project"
  location                    = var.region
  default_table_expiration_ms = null
}

# 3. GKE Cluster (Standard)
resource "google_container_cluster" "primary" {
  name     = "aero-cluster"
  location = var.region

  # We can't create a cluster with no node pool defined, but we want to only use
  # separately managed node pools. So we create the smallest possible default
  # node pool and immediately delete it.
  remove_default_node_pool = true
  initial_node_count       = 1
}

resource "google_container_node_pool" "primary_preemptible_nodes" {
  name       = "aero-node-pool"
  location   = var.region
  cluster    = google_container_cluster.primary.name
  node_count = 2

  node_config {
    preemptible  = true
    machine_type = "e2-standard-4" # 4 vCPU, 16GB RAM

    # Google recommends custom service accounts that have cloud-platform scope and permissions granted via IAM Roles.
    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform"
    ]
  }
}
