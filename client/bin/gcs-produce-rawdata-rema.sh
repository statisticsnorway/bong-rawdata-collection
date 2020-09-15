#!/usr/bin/env bash

source "$PWD/bin/gcs-config-rema.sh"

source "$HOME/bin/bong_secret_env.sh"

source "$PWD/bin/gcs-bong-rema.sh" produce-rawdata rema-fs

