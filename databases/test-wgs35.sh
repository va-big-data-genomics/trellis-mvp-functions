INSTANCE_NAME=trellis-neo4j-test-wgs35
IMAGE_NAME=neo4j-32g
STABLE_COS=cos-stable-74-11895-125-0
NEO4J_VERSION=3.5.3
SERVICE_ACCOUNT=133012913968-compute@developer.gserviceaccount.com
DISK_NAME=disk-neo4j-test
PROJECT=***REMOVED***-test
IP_ADDRESS=35.197.4.223

gcloud compute \
    instances create-with-container ${INSTANCE_NAME} \
    --project=${PROJECT} \
    --zone=us-west1-a \
    --machine-type=n1-highmem-16 \
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
    --container-image=gcr.io/${PROJECT}/${IMAGE_NAME}:${NEO4J_VERSION} \
    --container-restart-policy=always \
    --disk "name=${DISK_NAME},device-name=${DISK_NAME},mode=rw,boot=no" \
    --container-mount-host-path host-path=/mnt/disks/${DISK_NAME},mount-path=/data --metadata ^:^startup-script="mkdir -p /mnt/disks/${DISK_NAME} && mount -o discard,defaults /dev/sdb /mnt/disks/${DISK_NAME}" \
    --address=${IP_ADDRESS} \
    --labels=container-os=${STABLE_COS}
