# Always use script location as reference point
DIR="$( dirname "${BASH_SOURCE[0]}" )"
cd ${DIR}/../

gcloud functions deploy \
    wgs-9000-match-object-patterns \
    --project ***REMOVED*** \
    --memory 128MB \
    --entry-point main \
    --runtime python37 \
    --trigger-resource ***REMOVED***-trellis \
    --trigger-event google.storage.object.finalize \
    --env-vars-file ../../credentials/prod-wgs-9000.yaml
