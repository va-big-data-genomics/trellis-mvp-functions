# Always use script location as reference point
DIR="$( dirname "${BASH_SOURCE[0]}" )"
cd ${DIR}/../

gcloud functions deploy \
    wgs-9000-db-add-object-from-personalis-qc \
    --project gbsc-gcp-project-mvp-dev \
    --memory 128MB \
    --entry-point add_obj_to_db \
    --runtime python37 \
    --trigger-resource gbsc-gcp-project-mvp-dev-from-personalis-qc \
    --trigger-event google.storage.object.finalize \
    --env-vars-file ../../credentials/dev-wgs-9000.yaml
