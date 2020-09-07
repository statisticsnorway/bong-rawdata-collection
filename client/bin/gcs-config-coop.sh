#!/usr/bin/env bash

source "$PWD/bin/gcs-config.sh"

LOCAL_SOURCE_FOLDER_MOUNT="$PWD/../coop/data"

SOURCE_RAWDATA_TOPIC="bong-coop-source-test"
TARGET_RAWDATA_TOPIC="bong-coop-target-test"

SOURCE_CSV_FILES="ssb_ove_coop_1m_oct.csv"
