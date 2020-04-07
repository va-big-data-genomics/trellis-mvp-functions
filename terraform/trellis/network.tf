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
resource "google_compute_subnetwork" "trellis-subnet" {
    name            = "trellis-us-west1"
    ip_cidr_range   = "10.138.0.0/16"
    region          = "us-west1"
    network         = google_compute_network.trellis-vpc-network.self_link
}

// Create serverless VPC connector
resource "google_vpc_access_connector" "serverless-connector" {
    name            = "trellis"
    region          = "us-central1"
    ip_cidr_range   = "10.8.0.0/28"
    network         = google_compute_network.trellis-vpc-network.name
}

// Firewall rules
resource "google_compute_firewall" "trellis-allow-serverless-neo4j" {
    name = "trellis-allow-serverless-neo4j"
    network = google_compute_network.trellis-vpc-network.self_link

    allow {
        protocol = "tcp"
        ports = ["7473", "7687"]
    }
    source_ranges = ["10.8.0.0/28"]
    target_tags = ["neo4j"]
}

/* Temporary shelving
resource "google_compute_firewall" "trellis-allow-bastion-neo4j" {
    name = "trellis-allow-bastion-neo4j"
    network = google_compute_network.trellis-vpc-network.self_link

    allow {
        protocol = "tcp"
        ports = ["7473", "7687"]
    }
    source_ranges = [google_compute_instance.bastion-node.network_interface.0.network_ip]
    source_tags = ["bastion"]
    target_tags = ["neo4j"]
}
*/

/* Temporary shelving
resource "google_compute_firewall" "trellis-allow-bastion-bastion" {
    name = "trellis-allow-bastion-bastion"
    network = google_compute_network.trellis-vpc-network.self_link

    allow {
        protocol = "tcp"
        ports = ["22"]
    }

    source_ranges = [var.external-bastion-ip]
    source_tags = ["bastion"]
    target_tags = ["bastion"]
}
*/

// DELETE FOR PRODUCTION
resource "google_compute_firewall" "trellis-allow-stanford-neo4j" {
    name = "trellis-allow-stanford-neo4j"
    network = google_compute_network.trellis-vpc-network.self_link

    allow {
        protocol = "tcp"
        ports = ["7687", "7473"]
    }

    source_ranges = ["171.64.0.0/14"]
    target_tags = ["neo4j"]
}

// DELETE FOR PRODUCTION
resource "google_compute_firewall" "trellis-allow-stanford-neo4j-ssh" {
    name = "trellis-allow-stanford-neo4j-ssh"
    network = google_compute_network.trellis-vpc-network.self_link

    allow {
        protocol = "tcp"
        ports = ["22"]
    }

    source_ranges = ["171.64.0.0/14"]
    target_tags = ["neo4j"]
}

/* COMMENTED OUT
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
*/

/* COMMENTED OUT
resource "google_compute_firewall" "firewall-allow-bastion-ssh" {
    name = "trellis-allow-bastion-ssh"
    network = google_compute_network.trellis-vpc-network.self_link

    allow {
        protocol = "tcp"
        ports = ["22"]
    }

    source_ranges = "{BASTION_2_IP}"

}
*/
