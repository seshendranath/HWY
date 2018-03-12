#!/usr/bin/env bash

sbt clean assembly
#scp ../IntegratedMarketing_Load/target/IntegratedMarketing_Load-assembly-1.0-SNAPSHOT.jar 10.120.4.184:/home/fafshar/IntegratedMarketing_Load
scp ../IntegratedMarketing_Load/target/IntegratedMarketing_Load-assembly-1.0-SNAPSHOT.jar 10.120.4.184:work/sendfact/
