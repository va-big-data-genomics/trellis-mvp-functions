/*
|--------------------------------------------------------------------------
| Bastion Node
|--------------------------------------------------------------------------
|
| Bastion node 
|
*/

// Variables
variable "bastion-ip" {
    type = string
}

resource "google_compute_instance" "bastion-node" {
    name = "bastion-external"
    machine_type = "n1-standard-1"
    zone = "us-west1-a"
    
    tags = ["bastion"]

    boot_disk {
        initialize_params {
            image = "debian-cloud/debian-9"
            size = "10"
        }
    }


    
    network_interface {
        network = google_compute_network.bastion-network.self_link
        subnetwork = google_compute_subnetwork.bastion-us-west1.self_link

        access_config {
            nat_ip = var.bastion-ip
        }
    }
}