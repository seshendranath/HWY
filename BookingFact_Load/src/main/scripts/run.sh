#!/usr/bin/env bash

export SPARK_MAJOR_VERSION=2

spark-submit \
--master yarn \
--deploy-mode client \
--queue  aeservices \
--driver-memory=10G \
--num-executors=50 \
--executor-cores=6 \
--executor-memory=15G \
--conf spark.dynamicAllocation.enabled=false \
--class com.homeaway.dataanalytics.AnalyticstaskApp \
--driver-java-options -Dspring.profiles.active=prod \
BookingFact_Load-0.0.1.jar
