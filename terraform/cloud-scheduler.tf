/*
|--------------------------------------------------------------------------
| Cloud Scheduler Jobs
|--------------------------------------------------------------------------
|
| Cloud Scheduler jobs (i.e CRON jobs) are used to send requests
| to Trellis. These CRON jobs are used to import object metadata
| to the database & periodically launch batch jobs.
|
*/

// TODO: Figure out how to do this
resource "google_cloud_scheduler_job" "cron-import-data-from-personalis" {
    name = "import-data-from-personalis"
    description = "Import from-personalis blob metadata to Neo4j"
    schedule = "0 0 25 12 0"

    pubsub_target {
        topic_name = google_pubsub_topic.pubsub-list-bucket-topic.self_link
        data = base64encode("\{'resource': 'bucket', 'gcp-metadata': \{'name': \"}}\"
    }
}
