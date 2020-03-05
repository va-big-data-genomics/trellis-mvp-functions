INSTANCE_NAME=trellis-neo4j-dev
IMAGE_NAME=neo4j-5g
STABLE_COS=cos-stable-72-11316-171-0
NEO4J_VERSION=3.5.4
SERVICE_ACCOUNT=339496813274-compute@developer.gserviceaccount.com
PROJECT_ID=gbsc-gcp-project-mvp-dev
ZONE=us-west1-a
MACHINE_TYPE=n1-highmem-2
EXTERNAL_IP=35.247.31.130
PRIVATE_IP=10.138.0.2
DISK_NAME=disk-neo4j-dev

gcloud compute \
    instances create-with-container ${INSTANCE_NAME} \
    --project=${PROJECT_ID} \
    --zone=${ZONE} \
    --machine-type=${MACHINE_TYPE} \
    --network=trellis-dev \
    --subnet=us-west1 \
    --network-tier=PREMIUM \
    --private-network-ip=${PRIVATE_IP} \
    --address=${EXTERNAL_IP} \
    --maintenance-policy=MIGRATE \
    --service-account=${SERVICE_ACCOUNT} \
    --scopes=https://www.googleapis.com/auth/cloud-platform \
    --tags=neo4j,https-server \
    --image=${STABLE_COS} \
    --image-project=cos-cloud \
    --boot-disk-size=10GB \
    --boot-disk-type=pd-standard \
    --boot-disk-device-name=instance-1 \
    --container-image=gcr.io/${PROJECT_ID}/${IMAGE_NAME}:${NEO4J_VERSION} \
    --container-restart-policy=always \
    --disk "name=${DISK_NAME},device-name=${DISK_NAME},mode=rw,boot=no" \
    --container-mount-host-path host-path=/mnt/disks/${DISK_NAME},mount-path=/data --metadata ^:^startup-script="mkdir -p /mnt/disks/${DISK_NAME} && mount -o discard,defaults /dev/sdb /mnt/disks/${DISK_NAME}" \
    --labels=container-os=${STABLE_COS}
