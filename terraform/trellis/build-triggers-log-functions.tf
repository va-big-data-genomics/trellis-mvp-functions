/*
|--------------------------------------------------------------------------
| Cloud Build Triggers
|--------------------------------------------------------------------------
|
| Deploy cloud functions
|
*/

resource "google_cloudbuild_trigger" "log-delete-instance" {
    provider    = google-beta
    name        = "gcf-log-delete-instance"
    
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
        _CREDENTIALS_BLOB   = google_storage_bucket_object.trellis-config.name
        _CREDENTIALS_BUCKET = google_storage_bucket.trellis.name
        _ENVIRONMENT        = "google-cloud"
        _TRIGGER_TOPIC      = module.delete_instance_topic.resource_name
    }

}

resource "google_cloudbuild_trigger" "log-insert-cromwell-instance" {
    provider    = google-beta
    name        = "gcf-log-insert-cromwell-instance"

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
        _CREDENTIALS_BLOB   = google_storage_bucket_object.trellis-config.name
        _CREDENTIALS_BUCKET = google_storage_bucket.trellis.name
        _ENVIRONMENT        = "google-cloud"
        _TRIGGER_TOPIC      = module.insert_cromwell_topic.resource_name
    }

}

resource "google_cloudbuild_trigger" "log-insert-trellis-instance" {
    provider    = google-beta
    name        = "gcf-log-insert-trellis-instance"

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
        _CREDENTIALS_BLOB   = google_storage_bucket_object.trellis-config.name
        _CREDENTIALS_BUCKET = google_storage_bucket.trellis.name
        _ENVIRONMENT        = "google-cloud"
        _TRIGGER_TOPIC      = module.insert_trellis_topic.resource_name
    }

}