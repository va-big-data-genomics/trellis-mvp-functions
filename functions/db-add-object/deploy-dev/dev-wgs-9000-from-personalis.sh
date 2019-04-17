# Always use script location as reference point
DIR="$( dirname "${BASH_SOURCE[0]}" )"
cd ${DIR}/../

gcloud functions deploy \
    wgs-9000-db-add-object-from-personalis \
    --project ***REMOVED***-dev \
    --memory 128MB \
    --max-instances 20 \
    --entry-point add_obj_to_db \
    --runtime python37 \
    --trigger-resource ***REMOVED***-dev-from-personalis \
    --trigger-event google.storage.object.finalize \
    --env-vars-file ../../credentials/dev-wgs-9000.yaml
