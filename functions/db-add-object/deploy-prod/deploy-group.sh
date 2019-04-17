gcloud functions deploy add-db-entry-group \
    --entry-point main \
    --runtime python37 \
    --trigger-resource ***REMOVED***-group \
    --trigger-event google.storage.object.finalize \
    --set-env-vars NEO4J_CREDENTIALS=***REMOVED***-trellis-test:wakefield/credentials/neo4j-test.json,PUBLISH_TOPIC=new-db-node,GOOGLE_CLOUD_PROJECT=***REMOVED***
