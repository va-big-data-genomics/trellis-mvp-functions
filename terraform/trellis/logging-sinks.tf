/*
|--------------------------------------------------------------------------
| Logging Sinks
|--------------------------------------------------------------------------
|
| Deploy logging sinks
|
*/

module "delete_instance_log" {
  source                 = "./google-log-export"
  destination_uri        = module.delete_instance_topic.destination_uri
  filter                 = <<EOT
resource.type = gce_instance AND 
protoPayload.serviceName = compute.googleapis.com 
AND protoPayload.request.@type = type.googleapis.com/compute.instances.delete 
AND protoPayload.resourceName:google-pipelines-worker
EOT
  log_sink_name          = "delete-pipelines-worker"
  // What is this?
  #parent_resource_id     = var.parent_resource_id
  parent_resource_id     = var.project
  parent_resource_type   = "project"
  unique_writer_identity = true
}

module "delete_instance_topic" {
  source                   = "./pubsub"
  project_id               = var.project
  topic_name               = "delete-pipelines-worker"
  log_sink_writer_identity = module.delete_instance_log.writer_identity
  create_subscriber        = false
}

module "insert_cromwell_log" {
  source                 = "./google-log-export"
  destination_uri        = module.insert_cromwell_topic.destination_uri
  filter                 = <<EOT
resource.type = gce_instance 
AND protoPayload.serviceName = compute.googleapis.com 
AND protoPayload.request.@type = type.googleapis.com/compute.instances.insert
AND protoPayload.request.networkInterfaces.network: networks/${google_compute_network.trellis-vpc-network.name}
AND protoPayload.resourceName:google-pipelines-worker
AND protoPayload.request.labels.key:cromwell-workflow-id
EOT
  log_sink_name          = "insert-cromwell-instance"
  parent_resource_id     = var.project
  parent_resource_type   = "project"
  unique_writer_identity = true
}

module "insert_cromwell_topic" {
  source                   = "./pubsub"
  project_id               = var.project
  topic_name               = "insert-cromwell-instance"
  log_sink_writer_identity = module.insert_cromwell_log.writer_identity
  create_subscriber        = false
}

module "insert_trellis_log" {
  source                 = "./google-log-export"
  destination_uri        = module.insert_trellis_topic.destination_uri
  filter                 = <<EOT
resource.type = gce_instance 
AND protoPayload.serviceName = compute.googleapis.com 
AND protoPayload.request.@type = type.googleapis.com/compute.instances.insert
AND protoPayload.request.networkInterfaces.network: networks/${google_compute_network.trellis-vpc-network.name}
AND protoPayload.resourceName:google-pipelines-worker
AND protoPayload.request.labels.key:trellis-id
EOT
  log_sink_name          = "insert-trellis-instance"
  parent_resource_id     = var.project
  parent_resource_type   = "project"
  unique_writer_identity = true
}

module "insert_trellis_topic" {
  source                   = "./pubsub"
  project_id               = var.project
  topic_name               = "insert-trellis-instance"
  log_sink_writer_identity = module.insert_trellis_log.writer_identity
  create_subscriber        = false
}
