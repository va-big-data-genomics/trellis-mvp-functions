# Always use script location as reference point
DIR="$( dirname "${BASH_SOURCE[0]}" )"
cd ${DIR}/../

gcloud beta functions deploy \
    wgs-9000-db-add-object-from-personalis \
    --project gbsc-gcp-project-mvp \
    --memory 128MB \
    --max-instances 20 \
    --timeout 120 \
    --entry-point add_obj_to_db \
    --runtime python37 \
    --trigger-resource gbsc-gcp-project-mvp-from-personalis \
    --trigger-event google.storage.object.finalize \
    --env-vars-file ../../credentials/prod-wgs-9000.yaml
