#!/usr/bin/env bash

CID=$(docker ps | awk '/docker-proxy/{print $1}')

docker exec -it ${CID} docker-pull.sh "$@"
