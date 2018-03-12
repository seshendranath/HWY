package com.homeaway.analyticsengineering.task

/**
  * Created by aguyyala on 10/19/17.
  */


import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SaveMode
import com.homeaway.analyticsengineering.AnalyticsTaskApp._
import com.homeaway.analyticsengineering.encrypt.main.utilities.Email.{Mail, send}
import com.homeaway.analyticsengineering.encrypt.main.utilities.Utility
import org.apache.spark.sql.functions._


class InitialEmailSendFact_Load extends Utility {

  def run(): Unit = {

    val jobId = jc.getJobId(conf("ie.jobName"), conf("ie.objectName"), conf("ie.jobName"))
    val (emailInstanceId, _) = jc.startJob(jobId)

    spark.conf.set("spark.sql.shuffle.partitions","1024")

    val sourceDataLocation = conf("s3aUri") + conf("omniBucket") + "/" + conf("de.omniPrefix")
    val sourceDataFormat = conf("sourceDataFormat")
    val targetDataFormat = conf("targetDataFormat")
    val targetDataLocation = conf("s3aUri") + conf("s3Bucket") + "/" + conf("aeTier1DB")+ "/" + conf("de.targetDataLocationPrefix")
    val targetDB = conf("aeTier1DB")
    val targetTable = conf("ie.objectName")
    val targetPartCols = Array("send_date")

    val timeFormat = "yyyy-MM-dd HH:mm:ss"
    val timeFormatter = new SimpleDateFormat(timeFormat)

    val cal = Calendar.getInstance()
    val startTimestamp = timeFormatter.format(cal.getTime)

    val sql = spark.sql _

    try {

      jc.logInfo(instanceId, s"START Historical Email sendFact Process", -1)

      log.info(s"Reading Source Data from location $sourceDataLocation")
      val maeWhole = spark.read.format(sourceDataFormat).load(sourceDataLocation).distinct()
      val mae = maeWhole.where("sourceemailrecipientid IS NOT NULL AND sourceemailrecipientid != ''").repartition(1000)
      mae.createOrReplaceTempView("mae")


      log.info("Reading unknown sourceemailrecipientids from source")
      val maeUnknown = maeWhole.where("sourceemailrecipientid IS NULL OR TRIM(sourceemailrecipientid) = ''")
      maeUnknown.createOrReplaceTempView("maeUnknown")


      log.info("Aggregating the Source Data by ActivityDate, AssetName and CampaignId")
      var query =
        """
          				  |SELECT
          				  |      sourceemailrecipientid
          				  |     ,sourceassetname
          				  |     ,sourcecampaignid
          				  |     ,MAX(dateid) AS sendDate
          				  |     ,COUNT(*) AS totalSendCount
          				  |FROM mae
          				  |WHERE activitytype = 'EmailSend'
          				  |GROUP BY 1, 2, 3
        				""".stripMargin

      log.info(s"Running Query: $query")
      val sends = sql(query)
      sends.createOrReplaceTempView("sends")


      query =
        """
          				  |SELECT
          				  |      sourceemailrecipientid
          				  |     ,activitytype
          				  |     ,sourceassetname
          				  |     ,sourcecampaignid
          				  |     ,MIN(CASE WHEN activitytype='EmailOpen' THEN dateid ELSE NULL END) AS firstOpenDate
          				  |     ,MIN(CASE WHEN activitytype='EmailClickthrough' THEN dateid ELSE NULL END) AS firstClickDate
          				  |     ,MIN(CASE WHEN activitytype='Bounceback' THEN dateid ELSE NULL END) AS firstBounceDate
          				  |     ,MIN(CASE WHEN activitytype='Subscribe' THEN dateid ELSE NULL END) AS firstSubscribeDate
          				  |     ,MIN(CASE WHEN activitytype='Unsubscribe' THEN dateid ELSE NULL END) AS firstUnsubscribeDate
          				  |     ,MIN(CASE WHEN activitytype='FormSubmit' THEN dateid ELSE NULL END) AS firstFormSubmitDate
          				  |     ,COUNT(*) AS totalCount
          				  |FROM mae
          				  |WHERE activitytype != 'EmailSend'
          				  |GROUP BY 1, 2, 3, 4
        				""".stripMargin

      log.info(s"Running Query: $query")
      log.info("Pivoting by ActivityType")
      val maeTmp = sql(query).groupBy("sourceemailrecipientid", "sourceassetname", "sourcecampaignid", "firstOpenDate", "firstClickDate", "firstBounceDate", "firstSubscribeDate", "firstUnsubscribeDate", "firstFormSubmitDate").pivot("activitytype").agg(sum("totalcount").as("count"))

      log.info("Filling Null values with 0")
      val maeStage = maeTmp.na.fill(0, Seq("bounceback", "emailclickthrough", "emailopen", "subscribe", "unsubscribe", "formsubmit"))
      maeStage.createOrReplaceTempView("madf_stage")

      log.info("Aggregating all activity type counts")
      query =
        """
          				  |SELECT
          				  |       sourceemailrecipientid
          				  |      ,sourceassetname
          				  |      ,sourcecampaignid
          				  |      ,MIN(firstOpenDate) AS firstOpenDate
          				  |      ,MIN(firstClickDate) AS firstClickDate
          				  |      ,MIN(firstBounceDate) AS firstBounceDate
          				  |      ,MIN(firstSubscribeDate) AS firstSubscribeDate
          				  |      ,MIN(firstUnsubscribeDate) AS firstUnsubscribeDate
          				  |      ,MIN(firstFormSubmitDate) AS firstFormSubmitDate
          				  |      ,MAX(EmailOpen) AS EmailOpen_count
          				  |      ,MAX(EmailClickthrough) AS EmailClickthrough_count
          				  |FROM madf_stage
          				  |GROUP BY 1, 2, 3
        				""".stripMargin

      log.info(s"Running Query: $query")
      val otherActivity = sql(query)
      otherActivity.createOrReplaceTempView("otherActivity")


      log.info("Building base SendFact by looking behind 90 days")
      query =
        """
          				  |(
          				  |SELECT
          				  |       COALESCE(a.sourceemailrecipientid, b.sourceemailrecipientid) AS source_email_recipient_id
          				  |      ,COALESCE(a.sourceassetname, b.sourceassetname) AS source_asset_name
          				  |      ,COALESCE(a.sourcecampaignid, b.sourcecampaignid) AS source_campaign_id
          				  |      ,CAST(COALESCE(sendDate, "99991231") AS INT) AS send_date
          				  |      ,firstOpenDate AS first_open_date
          				  |      ,firstClickDate AS first_click_date
          				  |      ,firstBounceDate AS first_bounce_date
          				  |      ,firstSubscribeDate AS first_subscribe_date
          				  |      ,firstUnsubscribeDate AS first_unsubscribe_date
          				  |      ,firstFormSubmitDate AS first_form_submit_date
          				  |      ,CAST(COALESCE(EmailOpen_count, 0) AS Int) AS email_open_count
          				  |      ,CAST(COALESCE(EmailClickthrough_count, 0) AS Int) AS email_click_count
          				  |FROM
          				  |(
          				  |SELECT
          				  |      sourceemailrecipientid
          				  |     ,sendDate
          				  |     ,sourceassetname
          				  |     ,sourcecampaignid
          				  |FROM sends)  a
          				  |FULL OUTER JOIN
          				  |(SELECT
          				  |       sourceemailrecipientid
          				  |      ,sourceassetname
          				  |      ,sourcecampaignid
          				  |      ,firstOpenDate
          				  |      ,firstClickDate
          				  |      ,firstBounceDate
          				  |      ,firstSubscribeDate
          				  |      ,firstUnsubscribeDate
          				  |      ,firstFormSubmitDate
          				  |      ,EmailOpen_count
          				  |      ,EmailClickthrough_count
          				  |FROM otherActivity) b
          				  |ON a.sourceemailrecipientid = b.sourceemailrecipientid
          				  |AND a.sourceassetname = b.sourceassetname
          				  |AND a.sourcecampaignid = b.sourcecampaignid
          				  |)
          				  |UNION ALL
          				  |(SELECT
          				  |      CONCAT(emailaddress, sourceactivityid) AS source_email_recipient_id
          				  |     ,sourceassetname AS source_asset_name
          				  |     ,sourcecampaignid AS source_campaign_id
          				  |     ,CAST("99991231" AS INT) AS send_date
          				  |     ,MIN(CASE WHEN activitytype='EmailOpen' THEN CAST(date_format(CAST(activitydate AS DATE), "yyyyMMdd") AS INT) ELSE NULL END) AS first_open_date
          				  |     ,MIN(CASE WHEN activitytype='EmailClickthrough' THEN CAST(date_format(CAST(activitydate AS DATE), "yyyyMMdd") AS INT) ELSE NULL END) AS first_click_date
          				  |     ,MIN(CASE WHEN activitytype='Bounceback' THEN CAST(date_format(CAST(activitydate AS DATE), "yyyyMMdd") AS INT) ELSE NULL END) AS first_bounce_date
          				  |     ,MIN(CASE WHEN activitytype='Subscribe' THEN CAST(date_format(CAST(activitydate AS DATE), "yyyyMMdd") AS INT) ELSE NULL END) AS first_subscribe_date
          				  |     ,MIN(CASE WHEN activitytype='Unsubscribe' THEN CAST(date_format(CAST(activitydate AS DATE), "yyyyMMdd") AS INT) ELSE NULL END) AS first_unsubscribe_date
          				  |     ,MIN(CASE WHEN activitytype='FormSubmit' THEN CAST(date_format(CAST(activitydate AS DATE), "yyyyMMdd") AS INT) ELSE NULL END) AS first_form_submit_date
          				  |     ,MAX(CASE WHEN activitytype='EmailOpen' THEN 1 ELSE 0 END) AS email_open_count
          				  |     ,MAX(CASE WHEN activitytype='EmailClickthrough' THEN 1 ELSE 0 END) AS email_click_count
          				  |FROM maeUnknown
          				  |GROUP BY 1, 2, 3, 4)
        				""".stripMargin

      log.info(s"Running Query: $query")
      val marketingActivityEmailSendFact = sql(query).repartition(targetPartCols.map(x => col(x)): _*)
      marketingActivityEmailSendFact.persist

      log.info(s"Saving Data to target Location: $targetDataLocation")
      saveDataFrameToDisk(marketingActivityEmailSendFact, SaveMode.Overwrite, targetDataFormat, targetDataLocation, targetPartCols)

      log.info("Check and Create Hive DDL")
      checkAndCreateHiveDDL(hiveMetaStore, targetDB, targetTable, targetDataFormat, targetDataLocation, getColsFromDF(marketingActivityEmailSendFact, targetPartCols), targetPartCols.map((_, "string")))

      log.info("Add Hive Partitions")
      addHivePartitions(hiveMetaStore, targetDB, targetTable, constructHivePartitions(marketingActivityEmailSendFact, targetPartCols, targetDataLocation))
      marketingActivityEmailSendFact.unpersist

      jc.logInfo(instanceId, "END Historic Email sendFact Process", -1)

      log.info(s"Updating Daily Email SendFact Process so that it can pick the Start Timestamp ($startTimestamp) from here")
      val dailyEmailJobId = jc.getJobId(conf("ie.dailyProcessName"), conf("ie.objectName"), conf("ie.dailyProcessName"))
      val (dailyInstanceId, _) = jc.startJob(dailyEmailJobId)
      jc.endJob(dailyInstanceId, 1, startTimestamp, startTimestamp)

      jc.endJob(emailInstanceId, 1)
      jc.endJob(instanceId, 1)


      log.info("Validation")

      val eloqua=spark.read.parquet("s3a://path/marketing/brand/marketingActivityEloqua").where("activitytype='EmailSend'")
      eloqua.createOrReplaceTempView("eloqua")

      query =
        """
          |SELECT a.table
          |      ,a.send_count
          |      ,b.open_count
          |FROM
          |(
          |SELECT
          |      'eloqua' AS table
          |     ,CAST(COUNT(*) AS string) AS send_count
          |FROM eloqua
          |WHERE activitytype='EmailSend'
          |) a
          |JOIN
          |(
          |SELECT
          |      'eloqua' AS table
          |     ,CAST(COUNT(*) AS string) AS open_count
          |FROM eloqua
          |WHERE activitytype='EmailOpen'
          |) b
          |ON a.table = b.table
        """.stripMargin

      log.info(s"Running Query: $query")
      val eloqua_count = sql(query)
      eloqua_count.persist
      eloqua_count.count

      spark.catalog.refreshTable(s"$targetDB.$targetTable")

      query =
        s"""
           |SELECT a.table
           |      ,a.send_count
           |      ,b.open_count
           |FROM
           |(
           |SELECT
           |      'email_send_fact' AS table
           |     ,CAST(COUNT(*) AS string) AS send_count
           |FROM $targetDB.$targetTable
           |WHERE send_date != ${conf("defaultDate")}
           |) a
           |JOIN
           |(
           |SELECT
           |      'email_send_fact' AS table
           |     ,CAST(SUM(email_open_count) AS string) AS open_count
           |FROM $targetDB.$targetTable
           |) b
           |ON a.table = b.table
        """.stripMargin

      log.info(s"Running Query: $query")
      val email_send_fact = sql(query)
      email_send_fact.persist
      email_send_fact.count


      val eloqua_metrics = transposeUDF(eloqua_count, Seq("table"))
      eloqua_metrics.createOrReplaceTempView("eloqua_metrics")

      val email_send_fact_metrics = transposeUDF(email_send_fact, Seq("table"))
      email_send_fact_metrics.createOrReplaceTempView("email_send_fact_metrics")

      query =
        s"""
           |SELECT
           |      a.column_name AS metric
           |     ,a.column_value AS eloqua
           |     ,b.column_value AS email_send_fact
           |     ,ROUND(((CAST(a.column_value AS Double) - CAST(b.column_value AS Double))/CAST(a.column_value AS Double)) * 100, 2) AS ${conf("thresholdColumn")}
           |FROM eloqua_metrics a JOIN email_send_fact_metrics b
           |ON a.column_name = b.column_name
           |WHERE ABS(ROUND(((CAST(a.column_value AS Double) - CAST(b.column_value AS Double))/CAST(a.column_value AS Double)) * 100, 2)) > ${conf("ie.errorThreshold")}
        """.stripMargin
      log.info(s"Running Query: $query")
      val df = sql(query)
      df.show(false)
      df.columns


      if (df.count > 0) {
        val htmlBody = df.collect.map { row =>
          val value = row.getAs(conf("thresholdColumn")).asInstanceOf[Long].abs; (if (value > conf("ie.errorThreshold").toInt) s"<tr bgcolor=${conf("red")}><td>") + row.toString.stripPrefix("[").stripSuffix("]").split(",").mkString("</td><td>") + "</td></tr>"
        }.mkString

        val rMsg =
          s"""
             |<style>
             |table, th, td {
             |    border-collapse: collapse;
             |    border: 2px solid black;
             |}
             |</style>
             |<table>
             |  <tr bgcolor=${conf("blue")}>
             |    <th>Metric</th>
             |    <th>Eloqua</th>
             |    <th>Email_Send_Fact</th>
             |    <th>Diff</th>
             |  </tr>
             |  $htmlBody
             |</table>
           """.stripMargin

        send a Mail(
          from = conf("from") -> conf("fromName"),
          to = conf("to").split(","),
          subject = conf("ie.subject"),
          message = conf("ie.message"),
          richMessage = Option(rMsg)
        )


      }


    }
    catch {
      case e: Throwable  =>
        log.info(s"Something went WRONG during the initial Email Send Fact run for Instance: $emailInstanceId")
        jc.endJob(emailInstanceId, -1)
        throw e
    }
  }


}