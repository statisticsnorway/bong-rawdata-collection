#!/usr/bin/env bash

if [ $# -eq 0 ]; then
  echo "usage:"
  echo ""
  echo "  docker-proxy-login.sh SA_JSON_FILE"
  echo ""
  exit 1
fi

CID=$(docker ps | awk '/docker-proxy/{print $1}')

docker exec -it ${CID} docker-login.sh "$1"
