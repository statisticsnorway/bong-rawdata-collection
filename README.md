# Bong Rawdata Collection

This project provides a CLI for converting CSV data to Rawdata, uploaded to a bucket at GCS. 
The Application is deployed on Docker Engine on-premise. 

The production of rawdata requires a two steps approach: 

First it consumes Bong CSV/XML files and imports all CSV data into a Postgres/Lmdb database.
Secondly, the rawdata producer generates Avro Rawdata files that is uploaded to Dapla GCS bucket.

## Modules

* Client - THe CLI Application
* Commons - Common files parsing, database and producer capabilities
* Bong producer modules: NG, Coop and Rema 1000

## How it works

The Bong data is stored as CSV and XML files. Ie. the NorgesGruppen (NG) Bong item lines are organized
in random order. Consequently, there is no natural grouping of bong items. The easiest way to provide
a natural grouping, is to compose a Primary Key that is sortable, and let the database backend handle
sequenced ordering. 

The RepositoryKey is declared as:

* Store ID
* Bong ID
* Timestamp
* Unique sequence number (ordinal)

Henceforth, bong key is sortable and enables each bong to be read sequentially.

## CLI

Please refer to the `client/bin/` and read the shell scripts for usage.
