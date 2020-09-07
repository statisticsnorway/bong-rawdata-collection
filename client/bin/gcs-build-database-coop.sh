#!/usr/bin/env bash

source "$PWD/bin/gcs-config-coop.sh"

source "$HOME/bin/bong_secret_env.sh"

source "$PWD/bin/gcs-bong.sh" build-database coop-postgres
