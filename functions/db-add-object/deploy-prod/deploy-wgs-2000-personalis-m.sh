gcloud functions deploy \
    wgs-2000-add-personalis-obj-to-db-m \
    --project ***REMOVED*** \
    --entry-point main \
    --runtime python37 \
    --trigger-resource ***REMOVED***-personalis \
    --trigger-event google.storage.object.metadataUpdate \
    --env-vars-file wgs-2000-credentials.yaml
