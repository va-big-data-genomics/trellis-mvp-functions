/*
|--------------------------------------------------------------------------
| Cloud Build Triggers
|--------------------------------------------------------------------------
|
| Deploy cloud functions
|
*

resource "google_cloudbuild_trigger" "log-delete-instance" {
    provider = google-beta
    
    github {
        owner = var.github-owner
        name  = var.github-repo
        push  {
            branch = var.github-branch-pattern
        }
    }
    
    included_files = ["functions/log-delete-instance/*"]

    filename = "functions/log-delete-instance/cloudbuild.yaml"

    substitutions = {
        _CREDENTIALS_BLOB   = "credentials/trellis.yaml"
        _CREDENTIALS_BUCKET = "${var.project}-trellis"
        _ENVIRONMENT        = "google-cloud"
        _TRIGGER_TOPIC      = google_pubsub_topic.log-delete-instance.name
    }

}

resource "google_cloudbuild_trigger" "log-insert-cromwell-instance" {
    provider = google-beta
    
    github {
        owner = var.github-owner
        name  = var.github-repo
        push  {
            branch = var.github-branch-pattern
        }
    }
    
    included_files = ["functions/log-insert-cromwell-instance/*"]

    filename = "functions/log-insert-cromwell-instance/cloudbuild.yaml"

    substitutions = {
        _CREDENTIALS_BLOB   = "credentials/trellis.yaml"
        _CREDENTIALS_BUCKET = "${var.project}-trellis"
        _ENVIRONMENT        = "google-cloud"
        _TRIGGER_TOPIC      = google_pubsub_topic.log-insert-cromwell-instance.name
    }

}

resource "google_cloudbuild_trigger" "log-insert-trellis-instance" {
    provider = google-beta
    
    github {
        owner = var.github-owner
        name  = var.github-repo
        push  {
            branch = var.github-branch-pattern
        }
    }
    
    included_files = ["functions/log-insert-trellis-instance/*"]

    filename = "functions/log-insert-trellis-instance/cloudbuild.yaml"

    substitutions = {
        _CREDENTIALS_BLOB   = "credentials/trellis.yaml"
        _CREDENTIALS_BUCKET = "${var.project}-trellis"
        _ENVIRONMENT        = "google-cloud"
        _TRIGGER_TOPIC      = google_pubsub_topic.log-insert-trellis-instance.name
    }

}