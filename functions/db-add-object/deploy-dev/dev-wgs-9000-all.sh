# Set environment variables
PROJECT=***REMOVED***-dev
MEMORY=128MB
INSTANCES=20
TIMEOUT=120
RUNTIME=python37
CREDENTIALS="../../credentials/dev-wgs-9000.yaml"
GCLOUD_INVOKE="gcloud beta"
ENTRYPOINT=add_obj_to_db

# Always use script location as reference point
DIR="$( dirname "${BASH_SOURCE[0]}" )"
cd ${DIR}/../

${GCLOUD_INVOKE} functions deploy \
    wgs-9000-db-add-object-from-personalis-meta \
    --project ${PROJECT} \
    --memory ${MEMORY} \
    --max-instances ${INSTANCES} \
    --timeout ${TIMEOUT} \
    --entry-point ${ENTRYPOINT} \
    --runtime ${RUNTIME} \
    --trigger-resource ***REMOVED***-dev-from-personalis \
    --trigger-event google.storage.object.metadataUpdate \
    --env-vars-file ${CREDENTIALS}

${GCLOUD_INVOKE} functions deploy \
    wgs-9000-db-add-object-from-personalis-qc-meta \
    --project ${PROJECT} \
    --memory ${MEMORY} \
    --max-instances ${INSTANCES} \
    --timeout ${TIMEOUT} \
    --entry-point ${ENTRYPOINT} \
    --runtime ${RUNTIME} \
    --trigger-resource ***REMOVED***-dev-from-personalis-qc \
    --trigger-event google.storage.object.metadataUpdate \
    --env-vars-file ${CREDENTIALS}

${GCLOUD_INVOKE} functions deploy \
    wgs-9000-db-add-object-from-personalis \
    --project ${PROJECT} \
    --memory ${MEMORY} \
    --max-instances ${INSTANCES} \
    --timeout ${TIMEOUT} \
    --entry-point ${ENTRYPOINT} \
    --runtime ${RUNTIME} \
    --trigger-resource ***REMOVED***-dev-from-personalis \
    --trigger-event google.storage.object.finalize \
    --env-vars-file ${CREDENTIALS}

${GCLOUD_INVOKE} functions deploy \
    wgs-9000-db-add-object-from-personalis-qc \
    --project ${PROJECT} \
    --memory ${MEMORY} \
    --max-instances ${INSTANCES} \
    --timeout ${TIMEOUT} \
    --entry-point ${ENTRYPOINT} \
    --runtime ${RUNTIME} \
    --trigger-resource ***REMOVED***-dev-from-personalis-qc \
    --trigger-event google.storage.object.finalize \
    --env-vars-file ${CREDENTIALS}
