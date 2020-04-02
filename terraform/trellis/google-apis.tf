
resource "google_project_service" "vpc-accesss" {
    service = "vpcaccess.googleapis.com"

    disable_dependent_services = false
    disable_on_destroy = false
}

resource "google_project_service" "cloud-scheduler" {
    service = "cloudscheduler.googleapis.com"

    disable_dependent_services = false
    disable_on_destroy = false
}

resource "google_project_service" "cloud-build" {
    service = "cloudbuild.googleapis.com"

    disable_dependent_services = false
    disable_on_destroy = false
}

resource "google_project_service" "app-engine" {
    service = "appengine.googleapis.com"

    disable_dependent_services = false
    disable_on_destroy = false
}