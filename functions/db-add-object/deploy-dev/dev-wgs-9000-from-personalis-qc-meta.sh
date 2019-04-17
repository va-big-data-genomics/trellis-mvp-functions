# Always use script location as reference point
DIR="$( dirname "${BASH_SOURCE[0]}" )"
cd ${DIR}/../

gcloud functions deploy \
    wgs-9000-add-from-personalis-qc-obj-to-db-meta \
    --project gbsc-gcp-project-mvp-dev \
    --memory 128MB \
    --entry-point main \
    --runtime python37 \
    --trigger-resource gbsc-gcp-project-mvp-dev-from-personalis-qc \
    --trigger-event google.storage.object.metadataUpdate \
    --env-vars-file ../../credentials/dev-wgs-9000.yaml
