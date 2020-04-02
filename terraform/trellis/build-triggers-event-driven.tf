/*
|--------------------------------------------------------------------------
| Cloud Build Triggers
|--------------------------------------------------------------------------
|
| Deploy cloud functions for enabling request-driven database import.
|
*

resource "google_cloudbuild_trigger" "create-job-node" {
    provider = google-beta
    
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
        _CREDENTIALS_BLOB       = "credentials/trellis.yaml"
        _CREDENTIALS_BUCKET     = "${var.project}-trellis"
        _ENVIRONMENT            = "google-cloud"
        _TRIGGER_TOPIC          = google_pubsub_topic.create-job-node.name
    }
}

resource "google_cloudbuild_trigger" "check-triggers" {
    provider = google-beta
    
    github {
        owner = var.github-owner
        name  = var.github-repo
        push  {
            branch = var.github-branch-pattern
        }
    }
    
    included_files = [
        "functions/create-triggers/*",
        "config/phase3/database-triggers.py"
    ]

    filename = "functions/check-triggers/cloudbuild.yaml"

    substitutions = {
        _CREDENTIALS_BLOB       = "credentials/trellis.yaml"
        _CREDENTIALS_BUCKET     = "${var.project}-trellis"
        _ENVIRONMENT            = "google-cloud"
        _TRIGGER_TOPIC          = google_pubsub_topic.check-triggers.name
    }
}

resource "google_cloudbuild_trigger" "kill-job" {
    provider = google-beta
    
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
    provider = google-beta
    
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
        _CREDENTIALS_BLOB       = "credentials/trellis.yaml"
        _CREDENTIALS_BUCKET     = "${var.project}-trellis"
        _ENVIRONMENT            = "google-cloud"
        _TRIGGER_TOPIC          = google_pubsub_topic.db-query.name
    }
}

resource "google_cloudbuild_trigger" "check-dstat" {
    provider = google-beta
    
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
        _CREDENTIALS_BLOB       = "credentials/trellis.yaml"
        _CREDENTIALS_BUCKET     = "${var.project}-trellis"
        _ENVIRONMENT            = "google-cloud"
        _TRIGGER_TOPIC          = google_pubsub_topic.check-dstat.name
        _FUNCTION_NAME          = "check-dstat"
    }
}