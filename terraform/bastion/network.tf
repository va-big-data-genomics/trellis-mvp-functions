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

// Variables
variable "scg-ip" {
    type = string
}

// Create network
resource "google_compute_network" "bastion-network" {
    name = "trellis-bastion"
    auto_create_subnetworks = false
}    

// Create subnetwork
resource "google_compute_subnetwork" "bastion-us-west1" {
    name            = "trellis-bastion-us-west1"
    ip_cidr_range   = "10.0.0.0/9"
    region          = "us-west1"
    network         = google_compute_network.bastion-network.self_link
}

// Create firewall rule
resource "google_compute_firewall" "trellis-allow-scg-bastion-ssh" {
    name = "trellis-allow-scg-bastion"
    network = google_compute_network.bastion-network.self_link

    allow {
        protocol = "tcp"
        ports = ["22"]
    }
    source_ranges = [var.scg-ip]
    target_tags = ["bastion"]
}
