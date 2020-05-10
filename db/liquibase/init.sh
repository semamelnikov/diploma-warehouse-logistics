#!/usr/bin/env bash

/opt/liquibase/liquibase \
    --driver=org.postgresql.Driver \
    --url=jdbc:postgresql://postgres:5432/warehouse-db \
    --classpath=/usr/share/java/postgresql.jar \
    --changeLogFile=/scripts/changelog/db.changelog-master.xml \
    --username=postgres \
    --password=postgres \
    --contexts=all \
    update
