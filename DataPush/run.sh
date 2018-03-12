#!/usr/bin/env bash

sbt clean assembly
scp ./target/DataPush-assembly-1.0-SNAPSHOT.jar 10.120.4.184:s3Load/
