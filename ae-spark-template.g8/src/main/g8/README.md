# $name$
This project contains all the details pertaining to $name$

## Usage

* Run sbt assembly to build the jar

## Deployment

Prod: 
```

spark-submit \
--name $name$ \
--master yarn \
--deploy-mode client \
--driver-memory=10G \
--num-executors=5 \
--executor-cores=3 \
--executor-memory=2G \
$name$-assembly-1.0-SNAPSHOT.jar -e=prod
```
