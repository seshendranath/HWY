#!/usr/bin/env bash

cluster=ae_adhoc_spark

QDS_API_URL="https://us.qubole.com/api"

QDS_DEV_API_TOKEN=$1
dev_loc="s3://.../auxiliary_jars"

QDS_PROD_API_TOKEN=$2
prod_loc="s3://.../auxiliary_jars"


qd="$(qds.py --token $QDS_DEV_API_TOKEN --url $QDS_API_URL cluster status $cluster | jq -r '.nodes | .[] | select(.role == "master") | .private_ip' | sed 's/[-]/./g' | awk -F'.ec2' '{ gsub("ip.","",$1); print $1 }')"

echo "Copying Jars Dir to Qubole Dev Box $qd"
scp -r jars ${qd}:

echo "Uploading Jars Dir from Qubole Dev Box $qd to AWS $dev_loc"
ssh $qd "aws s3 cp jars $dev_loc --recursive"




qp="$(qds.py --token $QDS_PROD_API_TOKEN --url $QDS_API_URL cluster status $cluster | jq -r '.nodes | .[] | select(.role == "master") | .private_ip' | sed 's/[-]/./g' | awk -F'.ec2' '{ gsub("ip.","",$1); print $1 }')"

echo "Copying Jars Dir to Qubole prod Box $qp"
scp -r jars ${qp}:

echo "Uploading Jars Dir from Qubole prod Box $qp to AWS $prod_loc"
ssh $qp "aws s3 cp jars $prod_loc --recursive"