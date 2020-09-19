#!/usr/bin/env bash

mvn clean install -DskipTests -f ../pom.xml && \
mvn clean verify dependency:copy-dependencies -DskipTests && \
docker build -t rawdata-collection-client:dev -f Dockerfile .
