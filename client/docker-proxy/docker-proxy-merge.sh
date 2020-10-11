#!/usr/bin/env bash

if [ $# -eq 0 ]; then
  echo "usage:"
  echo ""
  echo "  docker-proxy-merge.sh PROD_BIP_IMAGE_NAME IMAGE_VER"
  echo ""
  exit 1
fi

REMOTE_IMAGE="eu.gcr.io/prod-bip/$1:$2"
LOCAL_IMAGE_FOLDER="$PWD/export"
LOCAL_IMAGE_FILE="$1_$2.tar"

now() {
  date +'%Y-%m-%d %H:%M:%S'
}

echo "[$(now)] [INFO] docker pull $REMOTE_IMAGE"
docker-proxy-pull.sh "$REMOTE_IMAGE"

echo "[$(now)] [INFO] docker save to $LOCAL_IMAGE_FILE"
docker-proxy-save.sh "$REMOTE_IMAGE" "$LOCAL_IMAGE_FILE"

echo "[$(now)] [INFO] docker load $LOCAL_IMAGE_FILE to $REMOTE_IMAGE"
docker load -i "$LOCAL_IMAGE_FOLDER/$LOCAL_IMAGE_FILE"
