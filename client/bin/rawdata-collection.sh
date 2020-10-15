#!/usr/bin/env bash

set +e

WORKDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Show usage
usage() {
  if [ -n "$1" ]; then
    echo ""
    echo "$1"
  fi

  cat <<EOP

$0 [options]

    -a | --action                  <action>                        (mandatory)
    -t | --target                  <target>                        (optional)
    -b | --bucket                  <bucket name>                   (mandatory)
   -sa | --service-account         <gcs service account json>      (optional)
   -to | --topic                   <rawdata topic>                 (mandatory)
  -msf | --mount-secret-folder     <mount secrets folder>          (optional)
  -rsf | --rawdata-secret-file     <encryption secrets>            (mandatory)
  -mcf | --mount-conf-folder       <mount conf folder>             (optional)
   -pf | --property-file           <property file>                 (optional)
  -msd | --mount-spec-folder       <mount spec folder>             (optional)
   -sf | --spec-file               <sepc file>                     (optional)
  -mif | --mount-source-folder     <mount import source folder>    (required)
  -csv | --csv-files               <import csv files (use: ,)>     (optional)
  -mef | --mount-export-folder     <mount avro export folder>      (optional)
   -dr | --dry-run                 <stop at number of iterations>  (optional)
    -h | --help

Example:

  $0 -a test-gcs-write -msf "$HOME/secrets" -sa sa_secret.json -b ssb-rawdata-prod-kag -to test-topic

EOP
  exit 0
}

if [ "$*" == "" ]; then
  usage Usage:
fi
# Read commandline arguments
while [ "$1" != "" ]; do
  case $1 in
  -a | --action)
    shift
    ACTION=$1
    ;;
  -t | --target)
    shift
    TARGET=$1
    ;;
  -b | --bucket)
    shift
    BUCKET_NAME=$1
    ;;
  -sa | --service-account)
    shift
    BUCKET_SA_FILE=$1
    ;;
  -to | --topic)
    shift
    TOPIC_NAME=$1
    ;;
  -msf | --mount-secret-folder)
    shift
    LOCAL_SECRET_FOLDER=$1
    ;;
  -rsf | --rawdata-secret-file)
    shift
    RAWDATA_SECRET_FILE=$1
    ;;
  -mcf | --mount-conf-folder)
    shift
    LOCAL_CONF_FOLDER=$1
    ;;
  -pf | --property-file)
    shift
    PROPERTY_FILE=$1
    ;;
  -msd | --mount-spec-folder)
    shift
    LOCAL_SPEC_FOLDER=$1
    ;;
  -sf | --spec-file)
    shift
    SPECIFICATION_FILE=$1
    ;;
  -mif | --mount-source-folder)
    shift
    LOCAL_SOURCE_FOLDER=$1
    ;;
  -csv | --csv-files)
    shift
    CSV_FILES=$1
    ;;
  -mef | --mount-export-folder)
    shift
    LOCAL_AVRO_FOLDER=$1
    ;;
  -dr | --dry-run)
    shift
    DRY_RUN=$1
    ;;
  -h | --help)
    usage
    exit
    ;;
  *)
    usage
    exit 1
    ;;
  esac
  shift
done

source "$WORKDIR/docker-collection-cli.sh"
