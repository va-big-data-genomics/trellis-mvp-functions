gcloud functions deploy \
    query-db-index-wgs-2000 \
    --project ***REMOVED*** \
    --entry-point main \
    --runtime python37 \
    --trigger-resource ***REMOVED***-trellis-test \
    --trigger-event google.storage.object.finalize \
    --env-vars-file wgs-2000-credentials.yaml 
