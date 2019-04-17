gcloud functions deploy \
    add-db-entry-meta-phase-2-data \
    --entry-point main \
    --runtime python37 \
    --trigger-resource ***REMOVED***-phase-2-data \
    --trigger-event google.storage.object.metadataUpdate \
    --env-vars-file wgs-2000-credentials.yaml
