provider "google" {
  project = "***REMOVED***"
  region  = "us-west1"
  zone    = "us-west1-b"
}

resource "google_compute_project_metadata" "default" {
    metadata = {
        trellis-network = "trellis"
        trellis-subnetwork = "us-west1"
    }
}

/*
|--------------------------------------------------------------------------
| Network Configuration
|--------------------------------------------------------------------------
|
| In order to run task VMs with only internal IP addresses, we need to 
| set up a Virtual Private Cloud (VPC) network. Here we will create a
| Trellis network, subnetwork, firewall rules, and a serverless VPC 
| connector.
|
*/

// Create network
resource "google_compute_network" "trellis-vpc-network" {
    name = "trellis"
    auto_create_subnetworks = false
}    

// Create subnetwork
resource "google_compute_subnetwork" "trellis-us-west1" {
    name            = "trellis-us-west1"
    ip_cidr_range   = "10.138.0.0/16"
    region          = "us-west1"
    network         = google_compute_network.trellis-vpc-network.self_link
}

// Create serverless VPC connector
resource "google_vpc_access_connector" "serverless-connector" {
    name = "trellis"
    region = "us-central1"
    ip_cidr_range = "10.8.0.0/28"
    network = google_compute_network.trellis-vpc-network.self_link
}

// Firewall rules
resource "google_compute_firewall" "trellis-allow-serverless-neo4j" {
    name = "trellis-allow-serverless-neo4j"
    network = google_compute_network.trellis-vcp-network.self_link

    allow {
        protocol = "tcp"
        ports = ["7473", "7687"]
    }
    source_ranges = "10.8.0.0/28"
    target_tags = ["neo4j"]
}

resource "google_compute_firewall" "trellis-allow-bastion-neo4j" {
    name = "trellis-allow-bastion-neo4j"
    network = google_compute_network.trellis-vcp-network.self_link

    allow {
        protocol = "tcp"
        ports = ["7473", "7687"]
    }
    source_ranges = "{BASTION_IP}"
    source_tags = ["bastion"]
    target_tags = ["neo4j"]
}

resource "google_compute_firewall" "trellis-allow-bastion-bastion" {
    name = "trellis-allow-bastion-bastion"
    network = google_compute_network.trellis-vcp-network.self_link

    allow {
        protocol = "tcp"
        ports = ["22"]
    }

    source_ranges = "{EXTERNAL_BASTION_IP}"
    source_tags = ["bastion"]
    target_tags = ["bastion"]
}

resource "google_compute_firewall" "trellis-allow-notebook-neo4j" {
    name = "trellis-allow-bastion-bastion"
    network = google_compute_network.trellis-vpc-network.self_link

    allow {
        protocol = "tcp"
        ports = ["7473","7687"]
    }

    source_ranges = "{AI_NOTEBOOK_IP}"
    source_tags = ["deeplearning-vm"]
    target_tags = ["neo4j"]
}

resource "google_compute_firewall" "firewall-allow-bastion-ssh" {
    name = "trellis-allow-bastion-ssh"
    network = google_compute_network.trellis-vpc-network.self_link

    allow {
        protocol = "tcp"
        ports = ["22"]
    }

    source_ranges = "{BASTION_2_IP}"

}
