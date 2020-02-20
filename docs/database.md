# Neo4j database deployment

These instructions describe how to deploy a Neo4j instance on Google Cloud Platform using the Neo4j community Docker image. Before doing this, we recommend setting up your VPC network via the network configuration instructions.

## A. Setup gcloud and Docker
1. Install Docker on your local machine: https://docs.docker.com/install/
2. Open a command-line terminal on your local machine and use the Docker utility to pull a Neo4j Docker image from DockerHub. We are pulling version 3.5.4 of the database since that it what we are using at the time of writing, but feel free to pull a newer version if you are feeling adventurous. A list of Neo4j Docker images can be found on their Dockerhub page: https://hub.docker.com/_/neo4j.

  ```
  docker pull neo4j:3.5.4
  ```

3. Make sure that your gcloud utility is point to the project you want to deploy Neo4j in. If not change it.

  ```
  gcloud info   # Check the Current Properties: [core] project: value
  gcloud config set project {my-project-id}   # Update project
  ```

4. Configure Docker to use gcloud according to steps 1-6 in the "Before you begin" section of this document: https://cloud.google.com/container-registry/docs/pushing-and-pulling. Alternately, complete all steps up to and including `gcloud auth configure-docker`.

## B. Customize your Neo4j image to use more memory
The default heap and page cache sizes for Neo4j images are 512 MB. If you are doing anything serious, this isn't going to cut it. To increase these sizes you will need to change the values specified in the neo4j.conf file.

Note: The community edition of Neo4j also limits you to using 4 CPUs to run the database. Unfortunately, you can't raise the CPU count without buying an Enterprise license. In practice, we've found that this limits you to about 20 concurrent database connections.

1. Clone the published Neo4j images Github repository.
```
git clone https://github.com/neo4j/docker-neo4j-publish
```

2. Find the directory of the version you want to use and make a copy of it. In this case I'm naming the copied directory with a '-32G' since I'm going to assign 32 GB of memory to heap/page cache. I want the database to be as performant as possible, so I'm giving it a ceiling of 64 GB of memory. You probably don't need this much. Consider starting with 5GB for heap/page cache, or even smaller. Give the same amount of memory to both properties, unless you know what you're doing. Navigate to the new directory.

```
cd docker-neo4j-publish
cp -r 3.5.4 3.5.4-32G
cd 3.5.4-32G/community
```

3. Open the docker entrypoint script in a text editor.

```
vi docker-entrypoint.sh
```

4. Increase the memory specified for heap and page cache. Specify the same values for heap_initial and heap_max.

```
: ${NEO4J_dbms_memory_pagecache_size:=32G}
: ${NEO4J_dbms_memory_heap_initial__size:=32G}
: ${NEO4J_dbms_memory_heap_max__size:=32G}
```
 
 5. Exit the text editor.
 
 ```
 :wq
 ```
 
 6. Build the docker image using the following command.
 
 ```
 docker build -t neo4j-32g:3.5.3 .
 ```

7. Tag the image with a Google Cloud Repository name & push it to your Google Container Registry. In order to use the Neo4j image, we'll want to add it to our project's private Container Registry. With the image in our local Container Registry, we won't need to expose our virtual machine to the internet to allow it to grab the Neo4j image from Dockerhub, eliminating a security vulnerability (TO DO: assess validity of this assertion).

  ```
  docker tag neo4j:3.5.4 gcr.io/{my-project-id}/neo4j:3.5.4
  docker push gcr.io/{my-project-id}/neo4j:3.5.4
  ```

## C. Create a persistent disk to store the database information
Using a persistent disk is optional but highly recommended. (INSERT REASONS TO USE A PERSISTENT DISK)
(Reference: https://medium.com/acoshift/running-postgresql-on-gce-container-vm-f1e25912365d)

1. Create a GCE persistent disk in the zone where you will create your Neo4j VM. 200 GB should be more that enough storage. We usually name disks by environment (e.g. "dev", "test", "prod"), but naming is arbitrary; call it what you want.

``` 
$ gcloud compute disks create disk-neo4j-{my-env-name} --size=200GB --type=pd-standard --zone=us-west1-a
Created [https://www.googleapis.com/compute/v1/projects/gbsc-gcp-project-mvp/zones/us-west1-a/disks/disk-neo4j-prod].
NAME             ZONE        SIZE_GB  TYPE         STATUS
disk-neo4j-{my-env-name}  us-west1-a  200      pd-standard  READY
```

2. (Optional) Add an existing snapshot policy. This will create snapshots of your disk on a regular basis, so you can revert to an earlier data state in case you encounter an issue with your database. More informatin can be found here: https://cloud.google.com/blog/products/compute/introducing-scheduled-snapshots-for-compute-engine-persistent-disk.
```
$ gcloud beta compute disks add-resource-policies disk-neo4j-{my-env-name} --resource-policies=weeklyschedule --zone=us-west1-a
```

3. New disks are unformatted. Create a new instance to format the disk (Reference: https://cloud.google.com/compute/docs/disks/add-persistent-disk#formatting).

```
$ gcloud compute instances create scratch-instance --machine-type f1-micro --zone=us-west1-a
Created [https://www.googleapis.com/compute/v1/projects/gbsc-gcp-project-mvp/zones/us-west1-a/instances/scratch-instance].
NAME              ZONE        MACHINE_TYPE  PREEMPTIBLE  INTERNAL_IP  EXTERNAL_IP     STATUS
scratch-instance  us-west1-a  f1-micro                   10.240.0.10  104.199.120.34  RUNNING
```

4. Attach the disk to the instance.
```
$ gcloud compute instances attach-disk scratch-instance --disk disk-neo4j-{my-env-name}
Updated [https://www.googleapis.com/compute/v1/projects/gbsc-gcp-project-mvp/zones/us-west1-a/instances/scratch-instance].
```

5. SSH into the instance
```
$ gcloud compute ssh scratch-instance
```

6. List disks. The attached disk should appear as "sdb".
```
$ lsblk
NAME   MAJ:MIN RM  SIZE RO TYPE MOUNTPOINT
sda      8:0    0   10G  0 disk
└─sda1   8:1    0   10G  0 part /
sdb      8:16   0  200G  0 disk
```

7. Format disk
```
$ sudo mkfs.ext4 -m 0 -F -E lazy_itable_init=0,lazy_journal_init=0,discard /dev/sdb
mke2fs 1.43.4 (31-Jan-2017)
Discarding device blocks: done
Creating filesystem with 52428800 4k blocks and 13107200 inodes
Filesystem UUID: 9fba5814-4add-499d-923c-51a73d5e858e
Superblock backups stored on blocks:
	32768, 98304, 163840, 229376, 294912, 819200, 884736, 1605632, 2654208,
	4096000, 7962624, 11239424, 20480000, 23887872

Allocating group tables: done
Writing inode tables: done
Creating journal (262144 blocks): done
Writing superblocks and filesystem accounting information: done
```

8. Exit VM
```
$ exit
logout
Connection to 104.199.120.34 closed.
```

9. Delete the instance
```
$ gcloud compute instances delete scratch-instance
The following instances will be deleted. Any attached disks configured
 to be auto-deleted will be deleted unless they are attached to any
other instances or the `--keep-disks` flag is given and specifies them
 for keeping. Deleting a disk is irreversible and any data on the disk
 will be lost.
 - [scratch-instance] in [us-west1-a]

Do you want to continue (Y/n)?  y

Deleted [https://www.googleapis.com/compute/v1/projects/gbsc-gcp-project-mvp/zones/us-west1-a/instances/scratch-instance].
```

## D. Create the Neo4j instance
