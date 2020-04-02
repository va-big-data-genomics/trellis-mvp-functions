/*
|--------------------------------------------------------------------------
| Cloud Storage Buckets
|--------------------------------------------------------------------------
|
| Deploy cloud functions
|
*/


resource "google_storage_bucket" "phase3-data-bucket" {
    name          = "${var.project}-from-personalis-phase3"
    location      = "US-WEST1"
    storage_class = "REGIONAL"
    labels = {
              user = "trellis",
              created_by = "terraform"
    }
}

resource "google_storage_bucket" "phase3-logs-bucket" {
    name          = "${var.project}-from-personalis-logs"
    location      = "US-WEST1"
    storage_class = "REGIONAL"
    labels = {
              user = "trellis",
              created_by = "terraform"
    }
}


resource "google_storage_bucket" "trellis-bucket" {
    name          = "${var.project}-trellis"
    location      = "US-WEST1"
    storage_class = "REGIONAL"
    labels = {
              user = "trellis",
              created_by = "terraform"
    }
}
