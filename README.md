# Bong Rawdata Collection

This project provides a CLI for converting CSV data to Rawdata, uploaded to a bucket at GCS. 
The Application is deployed on Docker Engine on-premise. 

The production of rawdata requires a two steps approach: 

First it consumes Bong CSV/XML files and imports all CSV data into a Postgres/Lmdb database.
Secondly, the rawdata producer generates Avro Rawdata files that is uploaded to Dapla GCS bucket.

## Modules

* Client - THe CLI Application
* API - Common files parsing, database and producer capabilities

## CLI

```
Usage:

bin/rawdata-collection.sh [options]

    -a | --action                  <action>                      (mandatory)
    -t | --target                  <target>                      (optional)
    -b | --bucket                  <bucket name>                 (mandatory)
   -sa | --service-account         <gcs service account json>    (optional)
   -to | --topic                   <rawdata topic>               (mandatory)
  -msf | --mount-secret-folder     <mount secrets folder>        (optional)
  -rsf | --rawdata-secret-file     <encryption secrets>          (mandatory)
  -mcf | --mount-conf-folder       <mount conf folder>           (optional)
   -pf | --property-file           <property file>               (optional)
  -msd | --mount-spec-folder       <mount spec folder>           (optional)
   -sf | --spec-file               <sepc file>                   (optional)
  -mif | --mount-source-folder     <mount import source folder>  (required)
  -csv | --csv-files               <import csv files (use: ,)>   (optional)
  -mef | --mount-export-folder     <mount avro export folder>    (optional)
    -h | --help

Example:

  bin/rawdata-collection.sh -a test-gcs-write -msf "$HOME/secrets" -sa sa_secret.json -b ssb-rawdata-prod-bucket -to test-topic

```

Please refer to the `client/bin/` and read `~/bin/rawdata-collection.sh` for usage.
