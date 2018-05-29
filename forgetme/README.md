# ForgetMe
This project contains all the details pertaining to ForgetMe

## Usage

* Run sbt assembly to build the jar

## Deployment

Prod: 
```
scala ForgetMe-assembly-1.0-SNAPSHOT.jar -e=prod --app=ae --debug --sqlserver.pwd=... 
```
