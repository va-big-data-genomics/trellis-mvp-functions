/*
|--------------------------------------------------------------------------
| Neo4j Database
|--------------------------------------------------------------------------
|
| The Neo4j database serves as the metadata store for Trellis.
|
*/

// Create disk

// Create disk snapshot schedule

resource "google_compute_instance" "neo4j-database" {
    name = "trellis-neo4j"
    machine_type = "n1-highmem-16"
    zone = "us-west1-a"
    
    tags = ["neo4j", "https-server"]

    boot_disk = {
        initialize_params {
            image = "cos-stable-74-11895-125-0"
            size = "10"
        }
    }

    attached_disks = {
        source = //INSERT LINK TO DISK
    }
    
    network_interface = {
        
