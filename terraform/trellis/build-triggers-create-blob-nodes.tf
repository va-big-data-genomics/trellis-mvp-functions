/*
|--------------------------------------------------------------------------
| Cloud Build Triggers
|--------------------------------------------------------------------------
|
| Deploy cloud functions for enabling request-driven database import.
|
*

resource "google_cloudbuild_trigger" "create-node-from-personalis-final" {
    provider = google-beta
    
    github {
        owner = var.github-owner
        name  = var.github-repo
        push  {
            branch = var.github-branch-pattern
        }
    }
    
    included_files = [
        "functions/create-blob-node/*",
        "config/phase3/from-personalis/*",
    ]

    filename = "functions/create-blob-node/cloudbuild.yaml"

    substitutions = {
        _BUCKET_SHORT_NAME      = "from-personalis"
        _CREDENTIALS_BLOB       = "credentials/trellis.yaml"
        _CREDENTIALS_BUCKET     = "${var.project}-trellis"
        _ENVIRONMENT            = "google-cloud"
        _OPERATION_SHORT_NAME   = "final"
        _TRELLIS_BUCKET         = "${var.project}-trellis"
        _TRIGGER_OPERATION      = "finalize"
        _TRIGGER_RESOURCE       = "${var.project}-from-personalis"
        _DATA_GROUP             = "phase3"
    }
}

resource "google_cloudbuild_trigger" "create-node-from-personalis-meta" {
    provider = google-beta
    
    github {
        owner = var.github-owner
        name  = var.github-repo
        push  {
            branch = var.github-branch-pattern
        }
    }
    
    included_files = [
        "functions/create-blob-node/*",
        "config/phase3/from-personalis/*",
    ]

    filename = "functions/create-blob-node/cloudbuild.yaml"

    substitutions = {
        _BUCKET_SHORT_NAME      = "from-personalis"
        _CREDENTIALS_BLOB       = "credentials/trellis.yaml"
        _CREDENTIALS_BUCKET     = "${var.project}-trellis"
        _ENVIRONMENT            = "google-cloud"
        _OPERATION_SHORT_NAME   = "meta"
        _TRELLIS_BUCKET         = "${var.project}-trellis"
        _TRIGGER_OPERATION      = "metadataUpdate"
        _TRIGGER_RESOURCE       = "${var.project}-from-personalis"
        _DATA_GROUP             = "phase3"
    }
}

resource "google_cloudbuild_trigger" "create-node-from-phase3-data-final" {
    provider = google-beta
    
    github {
        owner = var.github-owner
        name  = var.github-repo
        push  {
            branch = var.github-branch-pattern
        }
    }
    
    included_files = [
        "functions/create-blob-node/*",
        "config/phase3/from-personalis-phase3-data/*",
    ]

    filename = "functions/create-blob-node/cloudbuild.yaml"

    substitutions = {
        _BUCKET_SHORT_NAME      = "phase3-data"
        _CREDENTIALS_BLOB       = "credentials/trellis.yaml"
        _CREDENTIALS_BUCKET     = "${var.project}-trellis"
        _ENVIRONMENT            = "google-cloud"
        _OPERATION_SHORT_NAME   = "final"
        _TRELLIS_BUCKET         = "${var.project}-trellis"
        _TRIGGER_OPERATION      = "finalize"
        _TRIGGER_RESOURCE       = "${var.project}-phase3-data"
        _DATA_GROUP             = "phase3"
    }
}

resource "google_cloudbuild_trigger" "create-node-from-phase3-data-meta" {
    provider = google-beta
    
    github {
        owner = var.github-owner
        name  = var.github-repo
        push  {
            branch = var.github-branch-pattern
        }
    }
    
    included_files = [
        "functions/create-blob-node/*",
        "config/phase3/from-personalis-phase3-data/*",
    ]

    filename = "functions/create-blob-node/cloudbuild.yaml"

    substitutions = {
        _BUCKET_SHORT_NAME      = "phase3-data"
        _CREDENTIALS_BLOB       = "credentials/trellis.yaml"
        _CREDENTIALS_BUCKET     = "${var.project}-trellis"
        _ENVIRONMENT            = "google-cloud"
        _OPERATION_SHORT_NAME   = "meta"
        _TRELLIS_BUCKET         = "${var.project}-trellis"
        _TRIGGER_OPERATION      = "metadataUpdate"
        _TRIGGER_RESOURCE       = "${var.project}-phase3-data"
        _DATA_GROUP             = "phase3"
    }
}