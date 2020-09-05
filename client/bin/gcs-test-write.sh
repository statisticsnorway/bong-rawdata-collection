#!/usr/bin/env bash

IMAGE_NAME=bong-collection:dev
GCS_SECRET_JSON=ssb-team-dapla-rawdata-bong-dc28ff0c8faa.json
LOCAL_AVRO_FOLDER="$PWD/target/avro-test"
BUCKET_NAME=ssb-rawdata-prod-bong

docker run -it --rm \
  -e BONG_action=ping.test \
  -e BONG_target.gcs.service-account.key-file="/conf/$GCS_SECRET_JSON" \
  -e BONG_target.gcs.bucket-name="$BUCKET_NAME" \
  -e BONG_target.rawdata.topic=bong-ng-test \
  -e BONG_target.local-temp-folder=/avro/target/test \
  -v "$HOME/bin":/conf \
  -v "$LOCAL_AVRO_FOLDER":/avro \
  "$IMAGE_NAME"
