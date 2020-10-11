#!/usr/bin/env bash

# save image filename

docker save -o "/export/$2" "$1"
