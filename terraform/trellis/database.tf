/*
|--------------------------------------------------------------------------
| Project Configuration
|--------------------------------------------------------------------------
|
| Deploy a Neo4j graph database instance to act as the global metadata
| store for Trellis.
|
*/

module "gce-container" {
  source = "./google-container-vm"

  container = {
    image = "gcr.io/${var.project}/${var.neo4j-image}"

    env = [
      {
        "name"  = "NEO4J_dbms_memory_pagecache_size"
        "value" = var.neo4j-pagecache-size
      },
      {
        "name"  = "NEO4J_dbms_memory_heap_initial__size"
        "value" = var.neo4j-heap-size
      },
      {
        "name"  = "NEO4J_dbms_memory_heap_max__size"
        "value" = var.neo4j-heap-size
      },
    ]

    # Declare volumes to be mounted
    # This is similar to how Docker volumes are mounted
    volumeMounts = [
      /* Commented out
      {
        mountPath = "/cache"
        name      = "tempfs-0"
        readOnly  = false
      },
      */
      {
        mountPath = "/data"
        name      = "data-disk-0"
        readOnly  = false
      },
    ]
  }

  # Declare the volumes
  volumes = [
    /*
    {
      name = "tempfs-0"

      emptyDir = {
        medium = "Memory"
      }
    },
    */
    {
      name = "data-disk-0"

      gcePersistentDisk = {
        pdName = "data-disk-0"
        fsType = "ext4"
      }
    },
  ]

  restart_policy = "Always"
}

/* Create attached persistent disk
resource "google_compute_disk" "neo4j-data-disk" {
    name = "trellis-neo4j-db-disk"
    type = "pd-standard"
    size = 10
    image = "centos-6-v20200309"
    labels = {
        user = "trellis"
    }
}
*/


// Create Neo4j instance
resource "google_compute_instance" "neo4j-database" {
    name = "trellis-neo4j-db"
    machine_type = "n1-highmem-8"
    
    allow_stopping_for_update = true

    boot_disk {
        initialize_params {
            image = module.gce-container.source_image
        }
    }

    attached_disk {
        source      = "disk-trellis-neo4j-data"
        device_name = "data-disk-0"
        mode        = "READ_WRITE"
    }
    
    network_interface {
        network = google_compute_network.trellis-vpc-network.self_link
        subnetwork = google_compute_subnetwork.trellis-subnet.self_link
        access_config {}
    }

    metadata = {
        gce-container-declaration = module.gce-container.metadata_value
    }

    labels = {
        container-vm = module.gce-container.vm_container_label
    }

    tags = ["neo4j","https-server"]

    service_account {
        scopes = ["cloud-platform"]
    }
}
