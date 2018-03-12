package com.homeaway.analyticsengineering.task


import java.text.SimpleDateFormat
import java.util.Calendar
import com.homeaway.analyticsengineering.AnalyticsTaskApp._
import com.homeaway.analyticsengineering.encrypt.main.utilities.Utility
import org.apache.spark.sql.SaveMode


class InitialRLTLookup_Load extends Utility {


  def run(): Unit = {

    val jobId = jc.getJobId(conf("rlt.jobName"), conf("rlt.objectName"), conf("rlt.jobName"))
    val (rltInstanceId, _) = jc.startJob(jobId)

    val sourceDB = conf("rlt.sourceDB")
    val sourceTable = conf("rlt.sourceTable")
    val dailyJobName = conf("rlt.dailyJobName")
    val targetDataFormat = conf("targetDataFormat")
    val targetDataLocation = conf("s3aUri") + conf("s3StageBucket") + "/" + conf("targetDataLocationPrefix") + "/" + conf("rlt.objectName")
    val targetDB = conf("aeStageDB")
    val targetTable = conf("rlt.objectName")

    val timeFormat = "yyyy-MM-dd HH:mm:ss"
    val timeFormatter = new SimpleDateFormat(timeFormat)

    val cal = Calendar.getInstance()
    val startTimestamp = timeFormatter.format(cal.getTime)

    val sql = spark.sql _

    try {

      jc.logInfo(instanceId, s"START InitialEmailRLTDailyFact_Load Process", -1)

      log.info(s"Prepare RLT visit metrics Data")
      val query =
        s"""
           |SELECT
           |      CAST(dateid AS String) as dateid
           |     ,CASE WHEN
           |                fullvisitorid IS NULL
           |                OR lower(fullvisitorid) IN ('[unknown]', 'unknown','EDNFVID')
           |                THEN 'unknown' ELSE fullvisitorid END as fullvisitorid
           |     ,CASE WHEN
           |                visitid IS NULL
           |                OR lower(visitid) IN ('[unknown]', 'unknown','0')
           |                THEN 'unknown' ELSE visitid END as visitid
           |     ,MAX(CASE WHEN UPPER(rlt_campaign) in ('(NOT SET)', '%') THEN 'unknown' ELSE  UPPER(rlt_campaign) END) as rlt_campaign
           |     ,COUNT(*) as totalVisits
           |     ,MAX(CASE WHEN dated_search_page_count>0 THEN 1 ELSE 0 END) as datedSearch
           |     ,MAX(CASE WHEN (dated_search_page_count > 0 and pdp_view_count > 0) OR checkout_start_count > 0 OR inquiring_visit_count > 0  OR booking_request_count > 0 THEN 1 ELSE 0 END) as qualifiedVisit
           |FROM  $sourceDB.$sourceTable
           |WHERE rlt_marketingchannel='Email' AND rlt_marketingsubchannel='Email Marketing'
           |GROUP BY 1, 2, 3
        """.stripMargin

      log.info(s"Running Query: $query")
      val rlt = sql(query).coalesce(conf("rlt.numPartitions").toInt)
      rlt.createOrReplaceTempView("rlt")
      rlt.persist
      rlt.count


      log.info(s"Saving Data to target Location: $targetDataLocation")
      saveDataFrameToDisk(rlt, SaveMode.Overwrite, targetDataFormat, targetDataLocation)

      log.info(s"Check and Create Hive Table: $targetDB.$targetTable")
      checkAndCreateHiveDDL(hiveMetaStore, targetDB, targetTable, targetDataFormat, targetDataLocation, getColsFromDF(rlt))


      jc.logInfo(instanceId, "END InitialEmailRLTDailyFact_Load process", -1)

      log.info(s"Updating $dailyJobName Process so that it can pick the Start Timestamp ($startTimestamp) from here")
      val dailyEmailJobId = jc.getJobId(dailyJobName, targetTable, dailyJobName)
      val (dailyInstanceId, _) = jc.startJob(dailyEmailJobId)
      jc.endJob(dailyInstanceId, 1, startTimestamp, startTimestamp)

      jc.endJob(rltInstanceId, 1)
      jc.endJob(instanceId, 1)
    }
    catch {
      case e: Throwable  =>
        log.info(s"Something went WRONG during the RLT run for Instance: $rltInstanceId")
        jc.endJob(rltInstanceId, -1)
        throw e
    }

  }

}

