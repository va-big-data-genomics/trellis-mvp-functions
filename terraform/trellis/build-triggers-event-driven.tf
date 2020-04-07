/*
|--------------------------------------------------------------------------
| Cloud Build Triggers
|--------------------------------------------------------------------------
|
| Deploy cloud functions for enabling request-driven database import.
|
*/

resource "google_cloudbuild_trigger" "create-job-node" {
    provider    = google-beta
    name        = "gcf-create-job-node"
    
    github {
        owner = var.github-owner
        name  = var.github-repo
        push  {
            branch = var.github-branch-pattern
        }
    }
    
    included_files = [
        "functions/create-job-node/*",
    ]

    filename = "functions/create-job-node/cloudbuild.yaml"

    substitutions = {
        _CREDENTIALS_BLOB       = google_storage_bucket_object.trellis-config.name
        _CREDENTIALS_BUCKET     = google_storage_bucket.trellis.name
        _ENVIRONMENT            = "google-cloud"
        _TRIGGER_TOPIC          = google_pubsub_topic.create-job-node.name
    }
}

resource "google_cloudbuild_trigger" "check-triggers" {
    provider    = google-beta
    name        = "gcf-check-triggers"
    
    github {
        owner = var.github-owner
        name  = var.github-repo
        push  {
            branch = var.github-branch-pattern
        }
    }
    
    included_files = [
        "functions/create-triggers/*",
        "config/${var.data-group}/database-triggers.py"
    ]

    filename = "functions/check-triggers/cloudbuild.yaml"

    substitutions = {
        _CREDENTIALS_BLOB       = google_storage_bucket_object.trellis-config.name
        _CREDENTIALS_BUCKET     = google_storage_bucket.trellis.name
        _ENVIRONMENT            = "google-cloud"
        _TRIGGER_TOPIC          = google_pubsub_topic.check-triggers.name
        _DATA_GROUP             = var.data-group
    }
}

resource "google_cloudbuild_trigger" "kill-job" {
    provider    = google-beta
    name        = "gcf-kill-job"
    
    github {
        owner = var.github-owner
        name  = var.github-repo
        push  {
            branch = var.github-branch-pattern
        }
    }
    
    included_files = [
        "functions/kill-job/*",
    ]

    filename = "functions/check-triggers/cloudbuild.yaml"

    substitutions = {
        _ENVIRONMENT            = "google-cloud"
        _TRIGGER_TOPIC          = google_pubsub_topic.kill-job.name
    }
}

resource "google_cloudbuild_trigger" "db-query" {
    provider    = google-beta
    name        = "gcf-db-query"
    
    github {
        owner = var.github-owner
        name  = var.github-repo
        push  {
            branch = var.github-branch-pattern
        }
    }
    
    included_files = [
        "functions/db-query/*",
    ]

    filename = "functions/db-query/cloudbuild.yaml"

    substitutions = {
        _CREDENTIALS_BLOB       = google_storage_bucket_object.trellis-config.name
        _CREDENTIALS_BUCKET     = google_storage_bucket.trellis.name
        _ENVIRONMENT            = "google-cloud"
        _TRIGGER_TOPIC          = google_pubsub_topic.db-query.name
    }
}

resource "google_cloudbuild_trigger" "check-dstat" {
    provider    = google-beta
    name        = "gcr-check-dstat"
    
    github {
        owner = var.github-owner
        name  = var.github-repo
        push  {
            branch = var.github-branch-pattern
        }
    }
    
    included_files = [
        "functions/check-dstat/*",
    ]

    filename = "functions/check-dstat/cloudbuild.yaml"

    substitutions = {
        _CREDENTIALS_BLOB       = google_storage_bucket_object.trellis-config.name
        _CREDENTIALS_BUCKET     = google_storage_bucket.trellis.name
        _ENVIRONMENT            = "google-cloud"
        _TRIGGER_TOPIC          = google_pubsub_topic.check-dstat.name
        _FUNCTION_NAME          = "check-dstat"
    }
}