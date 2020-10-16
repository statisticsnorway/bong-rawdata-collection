#!/usr/bin/env bash

bin/rawdata-collection.sh \
  -a generate \
  -t dynamic-lmdb \
  -mif $HOME/IdeaProjects/ssb/rawdata-collection-client/api/src/test/resources/no/ssb/dc/collection/api/worker \
  -msd $HOME/IdeaProjects/ssb/rawdata-collection-client/api/src/test/resources/no/ssb/dc/collection/api/worker \
  -sf generic-spec.yaml \
  -mef $PWD/target \
  -to topic-test \
