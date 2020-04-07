/*
|--------------------------------------------------------------------------
| Project Configuration
|--------------------------------------------------------------------------
|
|
*/

provider "google" {
  project = var.project
  region  = var.region
  zone    = var.zone
}

provider "google-beta" {
  project = var.project
  region  = var.region
  zone    = var.zone
}


resource "google_compute_project_metadata" "default" {
    metadata = {
        trellis-network = google_compute_network.trellis-vpc-network.name
        trellis-subnetwork = google_compute_subnetwork.trellis-subnet.name
    }
}

/* Define this outside of Terraform
resource "google_compute_resource_policy" "daily_snapshot" {
  name   = "dailysnapshot"
  region = "us-west1"
  snapshot_schedule_policy {
    schedule {
      daily_schedule {
        days_in_cycle = 1
        start_time    = "04:00"
      }
    }
  }
}
*/