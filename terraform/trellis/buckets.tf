/*
|--------------------------------------------------------------------------
| Cloud Storage Buckets
|--------------------------------------------------------------------------
|
| Deploy cloud functions
|
*/


resource "google_storage_bucket" "phase3-data" {
    name          = "${var.project}-from-personalis-phase3"
    location      = "US-WEST1"
    storage_class = "REGIONAL"
    labels        = {
        user = "trellis",
        created_by = "terraform"
    }
}

resource "google_storage_bucket_object" "phase-3-data-readme" {
    name    = "README.txt"
    bucket  = google_storage_bucket.phase3-data.name
    content = <<EOT
This bucket contains Trellis outputs from processing MVP phase 3
whole-genome sequencing data from Personalis. This includes outputs of
the GATK germline somatic variant calling pipeline, as well as the 
following QC applications: FastQC, Samtools Flagstat, & Rtg-tools Vcfstats.

This bucket was automatically created during Trellis Terraforming.
EOT
}

resource "google_storage_bucket" "phase3-logs" {
    name          = "${var.project}-from-personalis-logs"
    location      = "US-WEST1"
    storage_class = "REGIONAL"
    labels = {
              user = "trellis",
              created_by = "terraform"
    }
}

resource "google_storage_bucket_object" "phase-3-logs-readme" {
    name    = "README.txt"
    bucket  = google_storage_bucket.phase3-logs.name
    content = <<EOT
This bucket contains the logs of Dsub jobs, initiated by Trellis, to
do processing of phase 3 whole-genome sequencing data from Personalis. 
Logs objects were stored in this bucket to avoid overloading the database 
with constant log update events.

This bucket was automatically created by Trellis Terraforming.
EOT
}


resource "google_storage_bucket" "trellis" {
    name          = "${var.project}-trellis"
    location      = "US-WEST1"
    storage_class = "REGIONAL"
    labels = {
              user = "trellis",
              created_by = "terraform"
    }
}

resource "google_storage_bucket_object" "trellis-readme" {
    name    = "README.txt"
    bucket  = google_storage_bucket.trellis.name
    content = <<EOT
This bucket contains configuration objects & miscellaneous other 
resources used for running Trellis.

This bucket was automatically created by Trellis Terraforming.
EOT
}
