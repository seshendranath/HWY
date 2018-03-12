SET hive.merge.mapfiles = true;
SET hive.merge.smallfiles.avgsize=3000000000;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions.pernode=10000;
SET hive.exec.max.dynamic.partitions=10000;
SET hive.exec.max.created.files=500000;
SET hive.tez.java.opts=-Xmx1640m;
SET tez.runtime.io.sort.mb=820;
SET tez.runtime.unordered.output.buffer.size-mb=205;
SET hive.execution.engine=tez;


INSERT OVERWRITE TABLE cedm.ce_scoring_engine PARTITION(scoredate)
SELECT
    value.entityid AS entityId
   ,value.modelid AS modelId
   ,value.modelname AS modelName
   ,scores.name AS scoreName
   ,AVG(scores.value) AS scoreValue
   ,'${hiveconf:end_date}' AS scoreDate
FROM
   daily.scoringengine_modelresult_changelog_global_daily
   LATERAL VIEW OUTER
       EXPLODE(value.scores) scoresTable AS scores
WHERE
    env='production'
    AND `date` >= '${hiveconf:start_date}' AND `date` <= '${hiveconf:end_date}'
    AND value.modelid IN ('276ef80f-40fa-4eec-b1cd-962277517519', '689538ae-c74e-485f-9fba-876a25416d9b', 'e4db28b8-fa50-4fef-9132-09c70038aa07')
    AND CASE WHEN value.modelname = 'tsf-v1_1' AND scores.name = 'tsf-score' AND scores.value > 0.5 THEN 1
             WHEN value.modelname = 'leeching-NA-v1_1' AND scores.name = 'LeecherAverageProbability' AND scores.value > 0.85 THEN 1
             WHEN value.modelname = 'leeching-EU-v1_1' AND scores.name = 'LeechingProbability' AND scores.value > 0.7 THEN 1
             ELSE 0 END = 1
GROUP BY
    value.entityid, value.modelid, value.modelname, scores.name, '${hiveconf:end_date}'
DISTRIBUTE BY '${hiveconf:end_date}'
;
