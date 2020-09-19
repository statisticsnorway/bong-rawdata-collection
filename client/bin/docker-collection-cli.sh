#!/usr/bin/env bash

WORKDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

RELEASE_IMAGE="statisticsnorway/rawdata-collection-client:0.1"
LOCAL_IMAGE="rawdata-collection-client:dev"
CONTAINER_IMAGE="$LOCAL_IMAGE"

if [ -z "$LOCAL_CONF_FOLDER" ]; then
  LOCAL_CONF_FOLDER="$WORKDIR/conf"
fi
if [ -z "$PROPERTY_FILE" ]; then
  PROPERTY_FILE="application-defaults.properties"
fi
LOCAL_DATABASE_VOLUME="source_database"
LOCAL_AVRO_VOLUME="avro_folder"
DEBUG_LOGGING=false

# Show usage
usage() {
  if [ -n "$ACTION" ]; then
    return
  fi

  cat <<EOP

Variables:

  ACTION               action                     (mandatory)
  TARGET               target                     (optional)
  RAWDATA_SECRET_FILE  full file path             (optional: ENCRYPTION_KEY & ENCRYPTION_SALT)
  BUCKET_SA_FILE       gcs service account json   (optional: json full filepath)
  BUCKET_NAME          gcs bucket name            (mandatory)
  TOPIC_NAME           rawdata topic              (mandatory)
  PROPERTY_FILE        filename under './conf'    (mandatory)
  LOCAL_SECRET_FOLDER  local secret mount folder  (mandatory)
  LOCAL_CONF_FOLDER    local conf mount folder    (mandatory)
  LOCAL_SOURCE_FOLDER  local import folder        (mandatory)
  CSV_FILES            import csv files           (optional)
  LOCAL_AVRO_FOLDER    local avro export folder   (optional: overrides rawdata client provider to filesystem)

Example:

  ACTION=action TARGET=target $0

EOP

  exit 0
}

log() {
  if [ "$DEBUG_LOGGING" = true ]; then
    echo "$1"
  fi
}

validate() {
  if [ -n "$RAWDATA_SECRET_FILE" ] && [ ! -f "$RAWDATA_SECRET_FILE" ]; then
    echo "Secret file is NOT found!"
    exit 0
  fi

  LOCAL_BUCKET_SA_FILEPATH="$LOCAL_SECRET_FOLDER/$BUCKET_SA_FILE"
  if [ -n "$LOCAL_BUCKET_SA_FILEPATH" ] && [ ! -f "$LOCAL_BUCKET_SA_FILEPATH" ]; then
    echo "Service account json file NOT found!"
    exit 0
  fi

  if [ -z "$BUCKET_NAME" ]; then
    echo "Bucket name is NOT set!"
    exit 0
  fi

  if [ -z "$TOPIC_NAME" ]; then
    echo "Rawdata topic is NOT set!"
    exit 0
  fi

  PROPERTY_FULL_FILE_PATH="$WORKDIR/conf/$PROPERTY_FILE"
  if [ -z "$PROPERTY_FULL_FILE_PATH" ] || [ ! -f "$PROPERTY_FULL_FILE_PATH" ]; then
    echo "Property file NOT found!"
    exit 0
  fi

  if [ -z "$LOCAL_SECRET_FOLDER" ] || [ ! -d "$LOCAL_SECRET_FOLDER" ]; then
    echo "Local secret directory NOT found!"
    exit 0
  fi

  if [ -n "$LOCAL_CONF_FOLDER" ] && [ ! -d "$LOCAL_CONF_FOLDER" ]; then
    echo "Local conf directory '$LOCAL_CONF_FOLDER' NOT found!"
    exit 0
  fi

  if [ -n "$LOCAL_SOURCE_FOLDER" ] && [ ! -d "$LOCAL_SOURCE_FOLDER" ]; then
    echo "Local source import directory NOT found!"
    exit 0
  fi
}

#
# Set bucket encryption keys
#
evaluateRawdataSecrets() {
  if [[ -f "$RAWDATA_SECRET_FILE" ]]; then
    echo "Set encryption environment variables from: $RAWDATA_SECRET_FILE"
    set -a
    source "$RAWDATA_SECRET_FILE"
    set +a
  fi
}

#
# Read property file and expand as docker environment variables
#
evaluateDockerEnvironmentVariables() {
  DOCKER_ENV_VARS=""

  if [ -n "$ENCRYPTION_KEY" ] && [ -n "$ENCRYPTION_SALT" ]; then
    DOCKER_ENV_VARS="-e BONG_target.rawdata.encryptionKey=$ENCRYPTION_KEY -e BONG_target.rawdata.encryptionSalt=$ENCRYPTION_SALT "
  fi

  if [ -n "$BUCKET_SA_FILE" ]; then
    CONTAINER_BUCKET_SA_FILEPATH="/secret/$BUCKET_SA_FILE"
    DOCKER_ENV_VARS="$DOCKER_ENV_VARS-e BONG_target.gcs.service-account.key-file=$CONTAINER_BUCKET_SA_FILEPATH "
  fi

  while read -r line; do
    ENV_VAR=$(eval echo "$line")
    if [[ ! "$ENV_VAR" = "#*" ]] && [[ -n "$ENV_VAR" ]]; then
      log "ENV_VAR: $ENV_VAR"
      DOCKER_ENV_VARS="$DOCKER_ENV_VARS-e BONG_${ENV_VAR} "
    fi
  done <"$WORKDIR/conf/$PROPERTY_FILE"

  # if local avro folder is set, use filesystem rawdata provider
  DOCKER_VOLUME_VARS=""
  if [ -n "$LOCAL_AVRO_FOLDER" ]; then
    DOCKER_ENV_VARS="$DOCKER_ENV_VARS-e BONG_target.rawdata.client.provider=filesystem"
    DOCKER_VOLUME_VARS="$DOCKER_VOLUME_VARS-v $LOCAL_AVRO_FOLDER:/export:Z "

  fi

  log "DOCKER_ENV_VARS: $DOCKER_ENV_VARS"
}

#
# Create Source Database and Target Avro docker volumes
#
createDockerVolumes() {
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
}

test_run() {
  if [ "$ACTION" = "test-gcs-write" ]; then
    docker run -it \
      -e BONG_action="$ACTION" \
      -e BONG_target="$TARGET" \
      ${DOCKER_ENV_VARS} \
      -v "$LOCAL_SECRET_FOLDER":/secret:Z \
      ${DOCKER_VOLUME_VARS} \
      ${CONTAINER_IMAGE}
    exit 0
  fi
}

run() {
  set -x
  docker run -it \
    -e BONG_action="$ACTION" \
    -e BONG_target="$TARGET" \
    ${DOCKER_ENV_VARS} \
    -v "$LOCAL_SECRET_FOLDER":/secret:Z \
    -v "$LOCAL_CONF_FOLDER":/conf:Z \
    -v "$LOCAL_SOURCE_FOLDER":/source:Z \
    -v "$LOCAL_DATABASE_VOLUME":/database \
    -v "$LOCAL_AVRO_VOLUME":/avro \
    ${DOCKER_VOLUME_VARS} \
    ${CONTAINER_IMAGE}
}

#
# Execute
#
usage
validate
evaluateRawdataSecrets
evaluateDockerEnvironmentVariables
createDockerVolumes
test_run
run
