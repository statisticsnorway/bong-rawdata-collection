#!/usr/bin/env bash

source "$PWD/bin/gcs-config.sh"

LOCAL_SOURCE_FOLDER_MOUNT="$PWD/../ng/data"

SOURCE_RAWDATA_TOPIC="bong-ng-source-test"
TARGET_RAWDATA_TOPIC="bong-ng-target-test"

SOURCE_CSV_FILES="ssb_ove_1m_oct.csv"
