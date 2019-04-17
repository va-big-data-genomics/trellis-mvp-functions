# Always use script location as reference point
DIR="$( dirname "${BASH_SOURCE[0]}" )"
cd ${DIR}/../

gcloud beta functions deploy \
    wgs-9000-db-add-object-from-personalis-qc \
    --project ***REMOVED*** \
    --memory 128MB \
    --max-instances 20 \
    --timeout 120 \
    --entry-point add_obj_to_db \
    --runtime python37 \
    --trigger-resource ***REMOVED***-from-personalis-qc \
    --trigger-event google.storage.object.finalize \
    --env-vars-file ../../credentials/prod-wgs-9000.yaml
