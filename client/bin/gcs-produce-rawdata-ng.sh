#!/usr/bin/env bash

source "$PWD/bin/gcs-config-ng.sh"

source "$HOME/bin/bong_secret_env.sh"

source "$PWD/bin/gcs-bong.sh" produce-rawdata ng-postgres

