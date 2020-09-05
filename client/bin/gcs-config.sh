#!/usr/bin/env bash

IMAGE_NAME=bong-collection:dev

LOCAL_CONF_FOLDER_MOUNT="$HOME/bin"
LOCAL_SOURCE_FOLDER_MOUNT="$PWD/../ng/data"
LOCAL_DATABASE_VOLUME="source_database"
LOCAL_AVRO_VOLUME="avro_folder"

SOURCE_RAWDATA_TOPIC="bong-ng-source-test"
TARGET_RAWDATA_TOPIC="bong-ng-target-test"

TARGET_BUCKET_NAME="ssb-rawdata-prod-bong"
TARGET_GCS_SECRET_JSON="ssb-team-dapla-rawdata-bong-dc28ff0c8faa.json"

LOCAL_DATABASE_VOLUME_EXISTS="$(docker volume ls -f "name=$LOCAL_DATABASE_VOLUME" -q)"
if [ -z "$LOCAL_DATABASE_VOLUME_EXISTS" ]; then
  echo "Create Volume: $LOCAL_DATABASE_VOLUME"
  docker volume create "$LOCAL_DATABASE_VOLUME"
fi

LOCAL_AVRO_VOLUME_EXISTS="$(docker volume ls -f "name=$LOCAL_AVRO_VOLUME" -q)"
if [ -z "$LOCAL_AVRO_VOLUME_EXISTS" ]; then
  echo "Create Volume: $LOCAL_AVRO_VOLUME"
  docker volume create "$LOCAL_AVRO_VOLUME"
fi
