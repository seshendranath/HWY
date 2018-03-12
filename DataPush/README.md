# DataPush
This project contains all the details pertaining to DataPush

## Usage

* Run sbt assembly to build the jar

## Deployment

Prod: 
```

spark-submit \
--name DataPush \
--master yarn \
--deploy-mode client \
--driver-memory=10G \
--num-executors=5 \
--executor-cores=3 \
--executor-memory=2G \
DataPush-assembly-1.0-SNAPSHOT.jar -e=prod
```
