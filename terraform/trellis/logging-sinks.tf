/*
|--------------------------------------------------------------------------
| Logging Sinks
|--------------------------------------------------------------------------
|
| Deploy logging sinks
|
*/

/* Don't have permission 
resource "google_project_iam_binding" "log-writer" {
    role = "roles/pubsub.publisher"

    members = [
        "serviceAccount:cloud-logs@system.gserviceaccount.com",
    ]
}
*/

resource "google_logging_project_sink" "sink-delete-pipelines-worker" {
    name = "sink-delete-pipelines-worker"

    # Can export to pubsub, cloud storage, or bigquery
    destination = "pubsub.googleapis.com/projects/${var.project}/topics/${google_pubsub_topic.log-delete-instance.name}"

    filter = <<EOT
resource.type = gce_instance AND 
protoPayload.serviceName = compute.googleapis.com 
AND protoPayload.request.@type = type.googleapis.com/compute.instances.delete 
AND protoPayload.resourceName:google-pipelines-worker
EOT

    unique_writer_identity = true


}

resource "google_logging_project_sink" "sink-insert-cromwell-instance" {
    name = "sink-insert-cromwell-instance"

    # Can export to pubsub, cloud storage, or bigquery
    destination = "pubsub.googleapis.com/projects/${var.project}/topics/${google_pubsub_topic.log-insert-cromwell-instance.name}"

    filter = <<EOT
resource.type = gce_instance 
AND protoPayload.serviceName = compute.googleapis.com 
AND protoPayload.request.@type = type.googleapis.com/compute.instances.insert
AND protoPayload.request.networkInterfaces.network: networks/${google_compute_network.trellis-vpc-network.name}
AND protoPayload.resourceName:google-pipelines-worker
AND protoPayload.request.labels.key:cromwell-workflow-id
EOT

    unique_writer_identity = true

}

resource "google_logging_project_sink" "sink-insert-trellis-instance" {
    name = "sink-insert-trellis-instance"

    # Can export to pubsub, cloud storage, or bigquery
    destination = "pubsub.googleapis.com/projects/${var.project}/topics/${google_pubsub_topic.log-insert-cromwell-instance.name}"

    filter = <<EOT
resource.type = gce_instance 
AND protoPayload.serviceName = compute.googleapis.com 
AND protoPayload.request.@type = type.googleapis.com/compute.instances.insert
AND protoPayload.request.networkInterfaces.network: networks/${google_compute_network.trellis-vpc-network.name}
AND protoPayload.resourceName:google-pipelines-worker
AND protoPayload.request.labels.key:trellis-id
EOT

    unique_writer_identity = true

}