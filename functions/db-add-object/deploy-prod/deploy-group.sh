gcloud functions deploy add-db-entry-group \
    --entry-point main \
    --runtime python37 \
    --trigger-resource gbsc-gcp-project-mvp-group \
    --trigger-event google.storage.object.finalize \
    --set-env-vars NEO4J_CREDENTIALS=gbsc-gcp-project-mvp-trellis-test:wakefield/credentials/neo4j-test.json,PUBLISH_TOPIC=new-db-node,GOOGLE_CLOUD_PROJECT=gbsc-gcp-project-mvp
