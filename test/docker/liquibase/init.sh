#!/usr/bin/env bash

/opt/liquibase/liquibase \
    --driver=org.postgresql.Driver \
    --url=jdbc:postgresql://postgres:5432/test-db \
    --classpath=/usr/share/java/postgresql.jar \
    --changeLogFile=/scripts/changelogs/initial-dbchangelog.xml \
    --username=postgres \
    --password=postgres \
    --contexts=all \
    update
