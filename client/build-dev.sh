#!/usr/bin/env bash

mvn clean install -DskipTests -f ../pom.xml && \
mvn clean verify dependency:copy-dependencies -DskipTests && \
docker build -t bong-collection:dev -f Dockerfile .
