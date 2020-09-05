#!/usr/bin/env bash

docker run -it --rm \
  -e BONG_action="$1" \
  -e BONG_target=ng.postgres \
  -e BONG_source.lmdb.path="/database" \
  -e BONG_source.postgres.driver.host="172.17.0.1" \
  -e BONG_source.rawdata.topic="$SOURCE_RAWDATA_TOPIC" \
  -e BONG_source.csv.filepath="/source" \
  -e BONG_source.csv.files="$SOURCE_CSV_NG_FILES" \
  -e BONG_target.rawdata.topic="$TARGET_RAWDATA_TOPIC" \
  -e BONG_target.gcs.bucket-name="$TARGET_BUCKET_NAME" \
  -e BONG_target.rawdata.encryptionKey="$BONG_PASSWORD" \
  -e BONG_target.rawdata.encryptionSalt="$BONG_SALT" \
  -e BONG_target.gcs.service-account.key-file="/conf/$TARGET_GCS_SECRET_JSON" \
  -e BONG_target.local-temp-folder="/avro/target" \
  -v "$LOCAL_CONF_FOLDER_MOUNT":/conf \
  -v "$LOCAL_SOURCE_FOLDER_MOUNT":/source \
  -v "$LOCAL_DATABASE_VOLUME":/database \
  -v "$LOCAL_AVRO_VOLUME":/avro \
  "$IMAGE_NAME"
