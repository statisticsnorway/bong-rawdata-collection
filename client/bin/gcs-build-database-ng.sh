#!/usr/bin/env bash

source "$PWD/bin/gcs-config.sh"

source "$HOME/bin/bong_secret_env.sh"

SOURCE_CSV_NG_FILES="ssb_ove_1m_oct.csv"

source "$PWD/bin/gcs-bong-ng.sh" buildDatabase
