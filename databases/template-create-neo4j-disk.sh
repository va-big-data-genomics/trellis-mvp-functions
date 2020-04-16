INSTANCE_NAME=neo4j-trellis-{unique_name}
IMAGE_NAME=
STABLE_COS=
NEO4J_VERSION=
SERVICE_ACCOUNT=
PROJECT_ID=
ZONE=
MACHINE_TYPE=
NETWORK=
SUBNET=
EXTERNAL_IP=
PRIVATE_IP=
DISK_NAME=

gcloud compute \
    instances create-with-container ${INSTANCE_NAME} \
    --project=${PROJECT_ID} \
    --zone=${ZONE} \
    --machine-type=${MACHINE_TYPE} \
    --network=${NETWORK} \
    --subnet=${SUBNET} \
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
