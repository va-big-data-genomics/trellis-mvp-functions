/*
|--------------------------------------------------------------------------
| Project Configuration
|--------------------------------------------------------------------------
|
|
*/

provider "google" {
  project = var.project
  region  = "us-west1"
  zone    = "us-west1-b"
}

provider "google-beta" {
  project = var.project
  region  = "us-west1"
  zone    = "us-west1-b"
}


resource "google_compute_project_metadata" "default" {
    metadata = {
        trellis-network = "trellis"
        trellis-subnetwork = "us-west1"
    }
}

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