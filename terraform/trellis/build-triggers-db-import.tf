/*
|--------------------------------------------------------------------------
| Cloud Build Triggers
|--------------------------------------------------------------------------
|
| Deploy cloud functions for enabling request-driven database import.
|
*

resource "google_cloudbuild_trigger" "list-bucket-page" {
    provider = google-beta
    
    github {
        owner = var.github-owner
        name  = var.github-repo
        push  {
            branch = var.github-branch-pattern
        }
    }
    
    included_files = ["functions/list-bucket-page/*"]

    filename = "functions/list-bucket-page/cloudbuild.yaml"

    substitutions = {
        _CREDENTIALS_BLOB   = "credentials/trellis.yaml"
        _CREDENTIALS_BUCKET = "${var.project}-trellis"
        _ENVIRONMENT        = "google-cloud"
        _TRIGGER_TOPIC      = google_pubsub_topic.list-bucket-page.name
    }
}

resource "google_cloudbuild_trigger" "match-blob-patterns" {
    provider = google-beta
    
    github {
        owner = var.github-owner
        name  = var.github-repo
        push  {
            branch = var.github-branch-pattern
        }
    }
    
    included_files = ["functions/match-blob-patterns/*"]

    filename = "functions/match-blob-patterns/cloudbuild.yaml"

    substitutions = {
        _CREDENTIALS_BLOB   = "credentials/trellis.yaml"
        _CREDENTIALS_BUCKET = "${var.project}-trellis"
        _ENVIRONMENT        = "google-cloud"
        _TRIGGER_TOPIC      = google_pubsub_topic.match-blob-patterns.name
        _DATA_GROUP         = "phase3"
    }
}

resource "google_cloudbuild_trigger" "db-query-index" {
    provider = google-beta
    
    github {
        owner = var.github-owner
        name  = var.github-repo
        push  {
            branch = var.github-branch-pattern
        }
    }
    
    included_files = ["functions/db-query-index/*"]

    filename = "functions/db-query-index/cloudbuild.yaml"

    substitutions = {
        _CREDENTIALS_BLOB   = "credentials/trellis.yaml"
        _CREDENTIALS_BUCKET = "${var.project}-trellis"
        _ENVIRONMENT        = "google-cloud"
        _TRIGGER_TOPIC      = google_pubsub_topic.db-query-index.name
    }
}

resource "google_cloudbuild_trigger" "update-metadata" {
    provider = google-beta
    
    github {
        owner = var.github-owner
        name  = var.github-repo
        push  {
            branch = var.github-branch-pattern
        }
    }
    
    included_files = ["functions/update-metadata/*"]

    filename = "functions/update-metadata/cloudbuild.yaml"

    substitutions = {
        _CREDENTIALS_BLOB   = "credentials/trellis.yaml"
        _CREDENTIALS_BUCKET = "${var.project}-trellis"
        _ENVIRONMENT        = "google-cloud"
        _TRIGGER_TOPIC      = google_pubsub_topic.update-metadata.name
    }
}
