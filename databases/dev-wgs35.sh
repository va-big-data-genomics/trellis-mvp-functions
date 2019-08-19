source ../credentials/dev-wgs-2019-db.sh

gcloud compute \
    instances create-with-container ${INSTANCE_NAME} \
    --project=${PROJECT} \
    --zone=us-west1-a \
    --machine-type=n1-highmem-2 \
    --network=default \
    --network-tier=PREMIUM \
    --maintenance-policy=MIGRATE \
    --service-account=${SERVICE_ACCOUNT} \
    --scopes=https://www.googleapis.com/auth/cloud-platform \
    --tags=neo4j,https-server \
    --image=${STABLE_COS} \
    --image-project=cos-cloud \
    --boot-disk-size=10GB \
    --boot-disk-type=pd-standard \
    --boot-disk-device-name=instance-1 \
    --container-image=gcr.io/***REMOVED***/${IMAGE_NAME}:${NEO4J_VERSION} \
    --container-restart-policy=always \
    --disk "name=${DISK_NAME},device-name=${DISK_NAME},mode=rw,boot=no" \
    --container-mount-host-path host-path=/mnt/disks/${DISK_NAME},mount-path=/data --metadata ^:^startup-script="mkdir -p /mnt/disks/${DISK_NAME} && mount -o discard,defaults /dev/sdb /mnt/disks/${DISK_NAME}" \
    --labels=container-os=${STABLE_COS}
