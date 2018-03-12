#!/bin/bash
#set -eo pipefail

#set -x
# Author: Ajay Guyyala
# This Script Loads CE Scoring Engine Data into Hive first and then copies data from Hive to SQL Server'

echo "Scoring Engine Job"

base_loc="/usr/local/homeaway/ae/eagle/analyticsengineering/ae-data-integration-templates/sql/cedm"
script_loc="/usr/local/homeaway/ae/eagle/analyticsengineering/ae-data-integration-templates/scripts"
log_loc="/home/aguyyala/ce/logs"
end_date=$(date -d "`date` -1 days" +%Y%m%d)
start_date=$(date -d "$end_date -6 days" +%Y%m%d)

echo "hive -hiveconf start_date=$start_date -hiveconf end_date=$end_date -f $base_loc/scoring_engine.hql 2>&1 | tee $log_loc/scoring_engine_${end_date}.log"

hive -hiveconf start_date=$start_date -hiveconf end_date=$end_date -f $base_loc/scoring_engine.hql 2>&1 | tee $log_loc/scoring_engine_${end_date}.log

if [ $? -ne 0 ]; then
        echo "Hive Job FAILED"
        exit 1
else
        echo "Hive Job is successful"
fi

echo "python $script_loc/task_template_data_process.py --table_name=cedm.dbo.cescoringengine --export_file=$base_loc/export_hive_sqlserver.sql --env=prod 2>&1 | tee $log_loc/scoring_engine_SQLLoad_${end_date}.log"

python $script_loc/task_template_data_process.py --table_name=cedm.dbo.cescoringengine --export_file=$base_loc/export_hive_sqlserver.sql --env=prod 2>&1 | tee $log_loc/scoring_engine_SQLLoad_${end_date}.log

if [ $? -ne 0 ]; then
        echo "SQL Load Job FAILED"
        exit 1
else
        echo "SQL Load Job is successful"
fi

