package com.homeaway.analyticsengineering.task


import java.text.SimpleDateFormat
import java.util.Calendar
import com.homeaway.analyticsengineering.AnalyticsTaskApp._
import com.homeaway.analyticsengineering.encrypt.main.utilities.Utility
import org.apache.spark.sql.SaveMode


class InitialGALookup_Load extends Utility {


  def run(): Unit = {

    val jobId = jc.getJobId(conf("ga.jobName"), conf("ga.objectName"), conf("ga.jobName"))
    val (gaInstanceId, _) = jc.startJob(jobId)

    val sourceDataLocation = conf("s3aUri") + conf("clickstreamBucket") + "/" + conf("ga.sourceDataLocationPrefix")
    val sourceDataFormat = conf("sourceDataFormat")
    val dailyJobName = conf("ga.dailyJobName")
    val targetDataFormat = conf("targetDataFormat")
    val targetDataLocation = conf("s3aUri") + conf("s3StageBucket") + "/" + conf("targetDataLocationPrefix") + "/" + conf("ga.objectName")
    val targetDB = conf("aeStageDB")
    val targetTable = conf("ga.objectName")

    val timeFormat = "yyyy-MM-dd HH:mm:ss"
    val timeFormatter = new SimpleDateFormat(timeFormat)

    val cal = Calendar.getInstance()
    val startTimestamp = timeFormatter.format(cal.getTime)

    val sql = spark.sql _

    try {

      jc.logInfo(instanceId, s"START InitialEmailGADailyFact_Load Process", -1)

      log.info(s"Read EnrichedEventsGA from $sourceDataLocation ")
      val clickstreamDF = spark.read.format(sourceDataFormat).load(sourceDataLocation).select("dateid", "utm_medium", "utm_source", "utm_campaign", "fullVisitorId", "visitId", "page_url_path", "page_url").where("utm_medium = 'email' AND lower(utm_source) IN ('cyc','cycle', 'adhoc','adh')")
      clickstreamDF.createOrReplaceTempView("clickstreamDF")


      log.info(s"Get haexternalsourceid's from Clickstream EnrichedEventsGA")
      val query =
        """
          |SELECT
          |        fullVisitorId
          |       ,visitId
          |       ,MAX(dateId) as dateId
          |       ,MAX(CASE WHEN UPPER(utm_campaign) in ('(NOT SET)', '%') THEN 'unknown' ELSE  UPPER(utm_campaign) END) as ga_campaign
          |       ,MAX(CASE WHEN length(haexternalsourceid)!=32 THEN null ELSE haexternalsourceid END) as haexternalsourceid
          |FROM (
          |SELECT  DISTINCT
          |        dateId
          |       ,fullVisitorId
          |       ,visitId
          |       ,utm_campaign
          |       ,SUBSTRING(TRIM(CASE WHEN PARSE_URL(lower(regexp_replace(page_url_path,"\\s+","")), 'QUERY', 'haexternalsourceid') is not null
          |                 THEN PARSE_URL(lower(regexp_replace(regexp_replace(page_url_path,"\\s+",""),"-","")), 'QUERY', 'haexternalsourceid')
          |                 WHEN PARSE_URL(lower(regexp_replace(page_url_path,"\\s+","")), 'QUERY', 'haexternalsourceid') is null
          |                      AND PARSE_URL(lower(regexp_replace(page_url,"\\s+","")), 'QUERY', 'haexternalsourceid') is NOT NULL
          |                 THEN PARSE_URL(lower(regexp_replace(regexp_replace(page_url,"\\s+",""),"-","")), 'QUERY', 'haexternalsourceid')
          |                 ELSE null END), 0, 32 ) AS haexternalsourceid
          |FROM clickstreamDF
          |)
          |GROUP BY 1,2
        """.stripMargin

      log.info(s"Running Query: $query")
      val ga_df = sql(query).coalesce(conf("ga.numPartitions").toInt)
      ga_df.persist
      ga_df.count
      ga_df.createOrReplaceTempView("ga_df")


      log.info(s"Saving Data to target Location: $targetDataLocation")
      saveDataFrameToDisk(ga_df, SaveMode.Overwrite, targetDataFormat, targetDataLocation)

      log.info(s"Check and Create Hive Table: $targetDB.$targetTable")
      checkAndCreateHiveDDL(hiveMetaStore, targetDB, targetTable, targetDataFormat, targetDataLocation, getColsFromDF(ga_df))

      jc.logInfo(instanceId, "END InitialEmailGADailyFact_Load process", -1)

      log.info(s"Updating $dailyJobName Process so that it can pick the Start Timestamp ($startTimestamp) from here")
      val dailyEmailJobId = jc.getJobId(dailyJobName, targetTable, dailyJobName)
      val (dailyInstanceId, _) = jc.startJob(dailyEmailJobId)
      jc.endJob(dailyInstanceId, 1, startTimestamp, startTimestamp)

      jc.endJob(gaInstanceId, 1)
      jc.endJob(instanceId, 1)
    }
    catch {
      case e: Throwable  =>
        log.info(s"Something went WRONG during the GA run for Instance: $gaInstanceId")
        jc.endJob(gaInstanceId, -1)
        throw e
    }

  }

}

