/*
|--------------------------------------------------------------------------
| Cloud Build Triggers
|--------------------------------------------------------------------------
|
| Deploy cloud functions for enabling request-driven database import.
|
*/

resource "google_cloudbuild_trigger" "launch-gatk-5-dollar" {
    provider    = google-beta
    name        = "gcf-launch-gatk-5-dollar"
    
    github {
        owner = var.github-owner
        name  = var.github-repo
        push  {
            branch = var.github-branch-pattern
        }
    }
    
    included_files = [
        "functions/launch-gatk-5-dollar/*",
    ]

    filename = "functions/launch-gatk-5-dollar/cloudbuild.yaml"

    substitutions = {
        _CREDENTIALS_BLOB       = google_storage_bucket_object.trellis-config.name
        _CREDENTIALS_BUCKET     = google_storage_bucket.trellis.name
        _ENVIRONMENT            = "google-cloud"
        _TRIGGER_TOPIC          = google_pubsub_topic.launch-gatk-5-dollar.name
    }
}

resource "google_cloudbuild_trigger" "launch-fastq-to-ubam" {
    provider    = google-beta
    name        = "gcf-launch-fastq-to-ubam"
    
    github {
        owner = var.github-owner
        name  = var.github-repo
        push  {
            branch = var.github-branch-pattern
        }
    }
    
    included_files = [
        "functions/launch-fastq-to-ubam/*",
    ]

    filename = "functions/launch-fastq-to-ubam/cloudbuild.yaml"

    substitutions = {
        _CREDENTIALS_BLOB       = google_storage_bucket_object.trellis-config.name
        _CREDENTIALS_BUCKET     = google_storage_bucket.trellis.name
        _ENVIRONMENT            = "google-cloud"
        _TRIGGER_TOPIC          = google_pubsub_topic.launch-fastq-to-ubam.name
    }
}

