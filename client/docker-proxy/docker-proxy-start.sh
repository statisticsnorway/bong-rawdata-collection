#!/usr/bin/env bash
set -e

IMAGE="docker-proxy:dev"

#HTTP_PROXY="host.docker.internal:3128"
#HTTPS_PROXY="$HTTP_PROXY"

LOCAL_SECRET_FOLDER="$HOME/bin"

DOCKER_OPTS=""
if [ -n "$HTTP_PROXY" ]; then
  DOCKER_OPTS="$DOCKER_OPTS-e HTTP_PROXY=$HTTP_PROXY "
fi

if [ -n "$HTTPS_PROXY" ]; then
  DOCKER_OPTS="$DOCKER_OPTS-e HTTPS_PROXY=$HTTPS_PROXY "
fi

createVolume() {
  LOCAL_DOCKER_VOLUME="docker_proxy_data"
  LOCAL_DOCKER_VOLUME_EXISTS="$(docker volume ls -f "name=$LOCAL_DOCKER_VOLUME" -q)"
  if [ -z "$LOCAL_DOCKER_VOLUME_EXISTS" ]; then
    echo "Create Volume: $LOCAL_DOCKER_VOLUME"
    docker volume create "$LOCAL_DOCKER_VOLUME"
  fi
}

createVolume

docker run --privileged -d ${DOCKER_OPTS} -v ${LOCAL_SECRET_FOLDER}:/secret:Z -v ${PWD}/export:/export:Z -v "$LOCAL_DOCKER_VOLUME":/var/lib/docker ${IMAGE}
