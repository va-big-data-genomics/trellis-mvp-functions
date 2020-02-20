# Neo4j database deployment

These instructions describe how to deploy a Neo4j instance on Google Cloud Platform using the Neo4j community Docker image. Before doing this, we recommend setting up your VPC network via the network configuration instructions.

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

5. Tag the image with a Google Cloud Repository name & push it to your Google Container Registry. In order to use the Neo4j image, we'll want to add it to our project's private Container Registry. With the image in our local Container Registry, we won't need to expose our virtual machine to the internet to allow it to grab the Neo4j image from Dockerhub, eliminating a security vulnerability (TO DO: assess validity of this assertion).

```
docker tag neo4j:3.5.4 gcr.io/{my-project-id}/neo4j:3.5.4
docker push gcr.io/{my-project-id}/neo4j:3.5.4
```

