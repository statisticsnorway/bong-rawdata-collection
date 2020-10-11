#!/usr/bin/env bash

SA_JSON="$(cat /secret/$1)"
echo "$SA_JSON" | docker login -u _json_key --password-stdin eu.gcr.io
