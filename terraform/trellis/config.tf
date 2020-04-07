/*
|--------------------------------------------------------------------------
| Trellis Configuration Object
|--------------------------------------------------------------------------
|
| Deploy logging sinks
|
*/

resource "google_storage_bucket_object" "trellis-config" {
  name   = "trellis-config.yaml"
  bucket = google_storage_bucket.trellis.name
  content = <<EOT
# Trellis variables
DATA_GROUP: ${var.data-group}
GOOGLE_CLOUD_PROJECT: ${var.project}
DATA_BUCKETS:
        - ${var.project}-from-personalis
        - ${var.project}-from-personalis-phase3
        - ${var.project}-from-personalis-phase3-logs
TRELLIS_BUCKET: ${google_storage_bucket.trellis.name}
BUCKET_PAGE_PREFIX: phase3/list-bucket-page
MATCHED_BLOBS_PREFIX: phase3/match-object-patterns

# Neo4j credentials
NEO4J_SCHEME: bolt
NEO4J_HOST: ${google_compute_instance.neo4j-database.network_interface.0.network_ip}
NEO4J_PORT: 7687
NEO4J_USER: neo4j
NEO4J_PASSPHRASE: LhkKB4L1U_dhYWR7yn5abl44Zmvych3fIMThdzyfeAu20S3_-dMJ3_P0R2K8t1NbxTf9-WWmKXeDJKH1O1copg
NEO4J_MAX_CONN: 20

# Dsub variables
DSUB_REGIONS: ${var.region}
DSUB_OUT_BUCKET: ${var.project}-from-personalis-phase3
DSUB_LOG_BUCKET: ${var.project}-from-personalis-phase3-logs
DSUB_OUT_ROOT: dsub
DSUB_USER: trellis
DSUB_NETWORK: ${google_compute_network.trellis-vpc-network.name}
DSUB_SUBNETWORK: ${google_compute_subnetwork.trellis-subnet.name}

GATK_MVP_DIR: workflow-inputs/gatk-mvp
GATK_MVP_HASH: 325c68c
GATK_GERMLINE_DIR: gatk-mvp-pipeline

# Trellis Pub/Sub topics
TOPIC_LIST_BUCKET_PAGE: ${google_pubsub_topic.list-bucket-page.name}
TOPIC_UPDATE_METADATA: ${google_pubsub_topic.update-metadata.name}
NEW_DB_NODE_TOPIC: ${google_pubsub_topic.create-blob-node.name}
NEW_JOBS_TOPIC: ${google_pubsub_topic.create-job-node.name}
DB_QUERY_TOPIC: ${google_pubsub_topic.db-query.name}
TOPIC_TRIGGERS: ${google_pubsub_topic.check-triggers.name}
TOPIC_KILL_JOB: ${google_pubsub_topic.kill-job.name}
TOPIC_FASTQ_TO_UBAM: ${google_pubsub_topic.launch-fastq-to-ubam.name}
TOPIC_GATK_5_DOLLAR: ${google_pubsub_topic.launch-gatk-5-dollar.name}
TOPIC_DSTAT: ${google_pubsub_topic.check-dstat.name}
TOPIC_BAM_FASTQC: ${google_pubsub_topic.launch-bam-fastqc.name}
TOPIC_FLAGSTAT: ${google_pubsub_topic.launch-flagstat.name}
TOPIC_VCFSTATS: ${google_pubsub_topic.launch-vcfstats.name}
TOPIC_TEXT_TO_TABLE: ${google_pubsub_topic.launch-text-to-table.name}
TOPIC_BIGQUERY_IMPORT_CSV: ${google_pubsub_topic.bigquery-import-csv.name}
TOPIC_BIGQUERY_APPEND_TSV: ${google_pubsub_topic.bigquery-append-tsv.name}

BIGQUERY_DATASET: mvp_trellis_${var.data-group}
EOT
}