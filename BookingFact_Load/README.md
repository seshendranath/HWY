# DistributionDailyFactLoad-application

This was planted using the __ae-data-integration__.

## Getting started

## Usage

__Compile__
```
./mvnw clean install
```

__Run locally__
This requires that you have [spark](http://spark.apache.org/downloads.html) (pre-built for hadoop) installed. We use the `spark-submit` tool to run the shaded jar created in this project. 
```
bin/spark-submit \
    --driver-java-options -Dspring.profiles.active=local \
    --class com.homeaway.DataAnalytics.AnalyticstaskApp \
    --master local[4] \ 
    <location of built jar>
```

__Run on stage__
You can run in Stage On prem cluster using below command
```
spark-submit \
    --driver-java-options -Dspring.profiles.active=stage \
    --class com.homeaway.DataAnalytics.AnalyticstaskApp \
    --master yarn \ 
    --classname=DistributionDailyFactLoad
    <location of built jar>
```
