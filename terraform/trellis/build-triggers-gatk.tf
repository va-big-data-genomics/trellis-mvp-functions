/*
|--------------------------------------------------------------------------
| Cloud Build Triggers
|--------------------------------------------------------------------------
|
| Deploy cloud functions
|
*/

resource "google_cloudbuild_trigger" "copy-gatk-to-trellis" {
    provider    = google-beta
    name        = "gcs-copy-gatk-to-trellis"

    github {
        owner = var.gatk-github-owner
        name  = var.gatk-github-repo
        push  {
            branch = var.gatk-github-branch-pattern
        }
    }
    
    #included_files = ["*"]

    filename = "trellis-cloudbuild.yaml"

    substitutions = {
        _BUCKET = google_storage_bucket.trellis.name
        _PATH   = google_storage_bucket_object.trellis-config.name
    }
}
