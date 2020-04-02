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