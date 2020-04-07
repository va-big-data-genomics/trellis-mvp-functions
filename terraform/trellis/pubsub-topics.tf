/*
|--------------------------------------------------------------------------
| Pub/Sub Topics
|--------------------------------------------------------------------------
|
| Pub/Sub topics
|
*/

resource "google_pubsub_topic" "bigquery-append-tsv" {
    name = "bigquery-append-tsv"
    labels = {user = "trellis"}
}

resource "google_pubsub_topic" "bigquery-import-csv" {
    name = "bigquery-import-csv"
    labels = {user = "trellis"}
}

resource "google_pubsub_topic" "check-dstat" {
    name = "check-dstat"
    labels = {user = "trellis"}
}

resource "google_pubsub_topic" "check-triggers" {
    name = "check-triggers"
    labels = {user = "trellis"}
}

resource "google_pubsub_topic" "create-blob-node" {
    name = "create-blob-node"
    labels = {user = "trellis"}
}

resource "google_pubsub_topic" "create-job-node" {
    name = "create-job-node"
    labels = {user = "trellis"}
}

resource "google_pubsub_topic" "db-query" {
    name = "db-query"
    labels = {user = "trellis"}
}

resource "google_pubsub_topic" "db-query-index" {
    name = "db-query-index"
    labels = {user = "trellis"}
}

resource "google_pubsub_topic" "kill-job" {
    name = "kill-job"
    labels = {user = "trellis"}
}

resource "google_pubsub_topic" "launch-bam-fastqc" {
    name = "launch-bam-fastqc"
    labels = {user = "trellis"}
}

resource "google_pubsub_topic" "launch-fastq-to-ubam" {
    name = "launch-fastq-to-ubam"
    labels = {user = "trellis"}
}

resource "google_pubsub_topic" "launch-flagstat" {
    name = "launch-flagstat"
    labels = {user = "trellis"}
}

resource "google_pubsub_topic" "launch-gatk-5-dollar" {
    name = "launch-gatk-5-dollar"
    labels = {user = "trellis"}
}

resource "google_pubsub_topic" "launch-text-to-table" {
    name = "launch-text-to-table"
    labels = {user = "trellis"}
}

resource "google_pubsub_topic" "launch-validate-cram" {
    name = "launch-validate-cram"
    labels = {user = "trellis"}
}

resource "google_pubsub_topic" "launch-vcfstats" {
    name = "launch-vcfstats"
    labels = {user = "trellis"}
}

resource "google_pubsub_topic" "list-bucket-page" {
    name = "list-bucket-page"
    labels = {user = "trellis"}
}

resource "google_pubsub_topic" "match-blob-patterns" {
    name = "match-blob-patterns"
    labels = {user = "trellis"}
}

resource "google_pubsub_topic" "update-metadata" {
    name = "update-metadata"
    labels = {user = "trellis"}
}