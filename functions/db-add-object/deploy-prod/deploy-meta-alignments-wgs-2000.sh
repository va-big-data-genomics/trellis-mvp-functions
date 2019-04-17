source secrets.txt

gcloud functions deploy \
    gcf-add-db-entry-wgs-2000-alignments-meta \
    --entry-point main \
    --runtime python37 \
    --trigger-resource gbsc-gcp-project-mvp-wgs-2000-alignments \
    --trigger-event google.storage.object.metadataUpdate \
    --set-env-vars NEO4J_URL=${NEO4J_URL} \
    --set-env-vars NEO4J_USER=${NEO4J_USER} \
    --set-env-vars NEO4J_PASSPHRASE=${NEO4J_PASS} \
    --set-env-vars PUBLISH_TOPIC=new-db-node \
    --set-env-vars GOOGLE_CLOUD_PROJECT=${PROJECT_ID}
