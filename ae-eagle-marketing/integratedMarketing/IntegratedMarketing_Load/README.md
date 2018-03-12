# IntegratedMarketing_Load
This project contains all the details pertaining to IntegratedMarketing_Load

## Usage

* Run sbt assembly to build the jar

## Deployment

Prod: 
```

spark-submit \
--name IntegratedMarketing_Load \
--master yarn \
--deploy-mode client \
--driver-memory=10G \
--num-executors=5 \
--executor-cores=3 \
--executor-memory=2G \
IntegratedMarketing_Load-assembly-1.0-SNAPSHOT.jar -e=prod
```
