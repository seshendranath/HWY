package com.homeaway.analyticsengineering.task

/**
  * Created by aguyyala on 10/19/17.
  */


import java.text.SimpleDateFormat
import com.amazonaws.services.s3.model.{ListObjectsV2Request, ListObjectsV2Result}
import com.github.nscala_time.time.Imports._
import com.homeaway.analyticsengineering.AnalyticsTaskApp._
import com.homeaway.analyticsengineering.encrypt.main.utilities.Email.{Mail, send}
import com.homeaway.analyticsengineering.encrypt.main.utilities.Utility
import org.apache.spark.sql.SaveMode
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
//import org.apache.commons.io.FileUtils
//import java.io.File
//import org.apache.hadoop.fs.Path
//import org.apache.spark.sql.functions._
//import com.amazonaws.services.s3.model.DeleteObjectsRequest
import org.apache.spark.sql.functions._

class DailyCampaignSendFact_Load extends Utility {

  def run(): Unit = {

    val jobId = jc.getJobId(conf("dc.jobName"), conf("dc.objectName"), conf("dc.jobName"))
    val (campaignInstanceId, campaignLastSuccessfulRunDetails) = jc.startJob(jobId)

    val attr = "Attributed"
    val targetDataFormat = conf("targetDataFormat")
    val targetDataLocation = conf("s3aUri") + conf("s3Bucket") + "/" + conf("aeTier1DB")+ "/" + conf("dc.targetDataLocationPrefix")
    val finalDataLocation = targetDataLocation + attr
    val tmpString = conf("tmpString")
    val targetDataTmpLocation = conf("hdfsLoc") + "/" + conf("aeTier1DB")+ "/" + conf("dc.targetDataLocationPrefix") + tmpString
    val targetDB = conf("aeTier1DB")
    val targetTable = conf("dc.objectName")
    val finalTable = targetTable + "_" + attr
    val sourceDB = conf("aeTier1DB")
    val sourceTable = conf("dc.sourceTable")
    val offset = conf("dc.offset").toInt
    val targetPartCols = Array("send_date")
    val sourceUnknownDataLocationPrefix = conf("aeTier1DB")+ "/" + conf("dc.sourceDataLocationPrefix") + "/" + targetPartCols.head + "=" + conf("defaultDate")
    val sourceUnknownDataLocation = conf("s3aUri") + conf("s3Bucket") + "/" + sourceUnknownDataLocationPrefix

    val sql = spark.sql _

    val dateFormat = "yyyyMMdd"
    val dateFormatter = DateTimeFormat.forPattern(dateFormat)

    val timeFormat = "yyyy-MM-dd HH:mm:ss"
    val timeFormatter = new SimpleDateFormat(timeFormat)

    val endDate = (DateTime.now - 1.day).toString(dateFormat)
    val startDate = DateTime.parse(endDate, dateFormatter).minusDays(offset).toString(dateFormat)


    val defaultStartTimestamp = (DateTime.now - 1.day).toString(timeFormat)
    val startTimestamp = campaignLastSuccessfulRunDetails.getOrElse("lastSuccessfulWaterMarkEndTime", defaultStartTimestamp)
    val endTimestamp = DateTime.now.toString(timeFormat)


    try {

      jc.logInfo(instanceId, s"START Daily Campaign sendFact Process", -1)

      log.info(s"Reading all Modified Files in $sourceUnknownDataLocation from the last timestamp $startTimestamp")

      val req = new ListObjectsV2Request().withBucketName(conf("s3Bucket")).withPrefix(sourceUnknownDataLocationPrefix+"/")
      var result = new ListObjectsV2Result
      val ftp = new ArrayBuffer[String]()
      val dtPattern = (targetPartCols.head + """=\d{8}""").r


      do {
        result = s3.listObjectsV2(req)
        ftp ++= result.getObjectSummaries.filter { x => val modifiedTime = timeFormatter.format(x.getLastModified.getTime); modifiedTime > startTimestamp && modifiedTime <= endTimestamp && !Set("success", "folder").exists(e => x.getKey.toLowerCase.matches(s".*$e.*")) }.map(x => conf("s3aUri") + conf("s3Bucket") + "/" + x.getKey)
        req.setContinuationToken(result.getNextContinuationToken)
      } while (result.isTruncated)

      val datesToProcess = ftp.map(dtPattern.findFirstIn(_).getOrElse(None)).distinct.filterNot(_ == None)
      val filesToProcess = ftp.toArray

      log.info(s"The following dates ${datesToProcess.mkString(",")} got modified for Unknown Data from the last run at $startTimestamp")
      if (datesToProcess.isEmpty || filesToProcess.isEmpty) {
        log.info("No Dates to Process for Unknown data. Either the threshold is too high or the Upstream didn't write any files")
        log.info("Creating maeUnknown Empty Dataframe")
        val maeUnknown = sql(s"SELECT * FROM $sourceDB.$sourceTable WHERE 1 = 2")
        maeUnknown.createOrReplaceTempView("maeUnknown")
      }
      else {
        log.info("Reading unknown sourceemailrecipientids from source")
        val maeUnknown = spark.read.format(conf("sourceDataFormat")).load(filesToProcess: _*).distinct
        maeUnknown.createOrReplaceTempView("maeUnknown")
      }

      log.info(s"""Reading and Aggregating all Unknown Data for date ${conf("defaultDate")}""")
      var query =
        s"""
           			   |SELECT
           			   |      send_date
           			   |     ,source_asset_name
           			   |     ,source_campaign_id
           			   |     ,SUM(send_count) AS send_count
           			   |     ,SUM(unique_open_count) AS unique_open_count
           			   |     ,SUM(unique_click_count) AS unique_click_count
           			   |     ,SUM(bounce_count) AS bounce_count
           			   |     ,SUM(subscribe_count) AS subscribe_count
           			   |     ,SUM(unsubscribe_count) AS unsubscribe_count
           			   |     ,SUM(form_submit_count) AS form_submit_count
           			   |     ,SUM(total_open_count) AS total_open_count
           			   |     ,SUM(total_click_count) AS total_click_count
           			   |FROM
           			   |(
           			   |SELECT
           			   |      send_date
           			   |     ,source_asset_name
           			   |     ,source_campaign_id
           			   |     ,send_count
           			   |     ,unique_open_count
           			   |     ,unique_click_count
           			   |     ,bounce_count
           			   |     ,subscribe_count
           			   |     ,unsubscribe_count
           			   |     ,form_submit_count
           			   |     ,total_open_count
           			   |     ,total_click_count
           			   |FROM $targetDB.$targetTable
           			   |WHERE send_date = ${conf("defaultDate")}
           			   |UNION ALL
           			   |SELECT
           			   |      CAST("${conf("defaultDate")}" AS INT) AS send_date
           			   |     ,source_asset_name
           			   |     ,source_campaign_id
           			   |     ,COUNT(*) AS send_count
           			   |     ,SUM(CASE WHEN first_open_date IS NOT NULL THEN 1 ELSE 0 END) AS unique_open_count
           			   |     ,SUM(CASE WHEN first_click_date IS NOT NULL THEN 1 ELSE 0 END) AS unique_click_count
           			   |     ,SUM(CASE WHEN first_bounce_date IS NOT NULL THEN 1 ELSE 0 END) AS bounce_count
           			   |     ,SUM(CASE WHEN first_subscribe_date IS NOT NULL THEN 1 ELSE 0 END) AS subscribe_count
           			   |     ,SUM(CASE WHEN first_unsubscribe_date IS NOT NULL THEN 1 ELSE 0 END) AS unsubscribe_count
           			   |     ,SUM(CASE WHEN first_form_submit_date IS NOT NULL THEN 1 ELSE 0 END) AS form_submit_count
           			   |     ,SUM(email_open_count) AS total_open_count
           			   |     ,SUM(email_click_count) AS total_click_count
           			   |FROM maeUnknown
           			   |GROUP BY 1, 2, 3
           			   |)
           			   |GROUP BY 1, 2, 3
				""".stripMargin

      log.info(s"Running Query: $query")
      val marketingActivityCampaignSendFactUnknown = sql(query).coalesce(1)
      marketingActivityCampaignSendFactUnknown.createOrReplaceTempView("marketingActivityCampaignSendFactUnknown")


      log.info(s"Reading and Aggregating Source Data by SendDate, AssetName and CampaignId for the last 90 days between $startDate AND $endDate")
      query =
        s"""
           			   |SELECT
           			   |      CAST(send_date AS Int) AS send_date
           			   |     ,source_asset_name
           			   |     ,source_campaign_id
           			   |     ,send_count
           			   |     ,unique_open_count
           			   |     ,unique_click_count
           			   |     ,bounce_count
           			   |     ,subscribe_count
           			   |     ,unsubscribe_count
           			   |     ,form_submit_count
           			   |     ,total_open_count
           			   |     ,total_click_count
           			   |FROM $targetDB.$targetTable
           			   |WHERE send_date NOT BETWEEN $startDate AND $endDate AND send_date != ${conf("defaultDate")}
           			   |UNION ALL
           			   |SELECT
           			   |      CAST(send_date AS Int) AS send_date
           			   |     ,source_asset_name
           			   |     ,source_campaign_id
           			   |     ,COUNT(*) AS send_count
           			   |     ,SUM(CASE WHEN first_open_date IS NOT NULL THEN 1 ELSE 0 END) AS unique_open_count
           			   |     ,SUM(CASE WHEN first_click_date IS NOT NULL THEN 1 ELSE 0 END) AS unique_click_count
           			   |     ,SUM(CASE WHEN first_bounce_date IS NOT NULL THEN 1 ELSE 0 END) AS bounce_count
           			   |     ,SUM(CASE WHEN first_subscribe_date IS NOT NULL THEN 1 ELSE 0 END) AS subscribe_count
           			   |     ,SUM(CASE WHEN first_unsubscribe_date IS NOT NULL THEN 1 ELSE 0 END) AS unsubscribe_count
           			   |     ,SUM(CASE WHEN first_form_submit_date IS NOT NULL THEN 1 ELSE 0 END) AS form_submit_count
           			   |     ,SUM(email_open_count) AS total_open_count
           			   |     ,SUM(email_click_count) AS total_click_count
           			   |FROM $sourceDB.$sourceTable
           			   |WHERE send_date BETWEEN $startDate AND $endDate
           			   |GROUP BY 1, 2, 3
           			   |UNION ALL
           			   |SELECT
           			   |      CAST(send_date AS Int) AS send_date
           			   |     ,source_asset_name
           			   |     ,source_campaign_id
           			   |     ,send_count
           			   |     ,unique_open_count
           			   |     ,unique_click_count
           			   |     ,bounce_count
           			   |     ,subscribe_count
           			   |     ,unsubscribe_count
           			   |     ,form_submit_count
           			   |     ,total_open_count
           			   |     ,total_click_count
           			   |FROM marketingActivityCampaignSendFactUnknown
				""".stripMargin

      log.info(s"Running Query: $query")
      val marketingActivityCampaignSendFact = sql(query).distinct.coalesce(1)
      marketingActivityCampaignSendFact.persist
      marketingActivityCampaignSendFact.createOrReplaceTempView("marketingActivityCampaignSendFact")


      log.info(s"Saving Data to TEMP target Location: $targetDataTmpLocation")
      saveDataFrameToDisk(marketingActivityCampaignSendFact, SaveMode.Overwrite, targetDataFormat, targetDataTmpLocation)

      log.info(s"Saving Data to Final target Location: $targetDataLocation")
      saveDataFrameToDisk(spark.read.format(targetDataFormat).load(targetDataTmpLocation).coalesce(1), SaveMode.Overwrite, targetDataFormat, targetDataLocation)

      log.info("Check and Create Hive DDL")
      checkAndCreateHiveDDL(hiveMetaStore, targetDB, targetTable, targetDataFormat, targetDataLocation, getColsFromDF(marketingActivityCampaignSendFact))


      log.info("Attributing Unknown Data on source_asset_name and source_campaign_id ")
      query =
        s"""
           			   |SELECT
           			   |      send_date
           			   |     ,source_asset_name
           			   |     ,source_campaign_id
           			   |     ,send_count
           			   |     ,unique_open_count
           			   |     ,unique_click_count
           			   |     ,bounce_count
           			   |     ,subscribe_count
           			   |     ,unsubscribe_count
           			   |     ,form_submit_count
           			   |     ,total_open_count
           			   |     ,total_click_count
           			   |FROM
           			   |(
           			   |SELECT
           			   |      COALESCE(orig_send_date, unknown_send_date) AS send_date
           			   |     ,source_asset_name
           			   |     ,source_campaign_id
           			   |     ,orig_send_count + unknown_send_count AS send_count
           			   |     ,orig_unique_open_count + unknown_unique_open_count AS unique_open_count
           			   |     ,orig_unique_click_count + unknown_unique_click_count AS unique_click_count
           			   |     ,orig_bounce_count + unknown_bounce_count AS bounce_count
           			   |     ,orig_subscribe_count + unknown_subscribe_count AS subscribe_count
           			   |     ,orig_unsubscribe_count + unknown_unsubscribe_count AS unsubscribe_count
           			   |     ,orig_form_submit_count + unknown_form_submit_count AS form_submit_count
           			   |     ,orig_total_open_count + unknown_total_open_count AS total_open_count
           			   |     ,orig_total_click_count + unknown_total_click_count AS total_click_count
           			   |     ,row_number() OVER (PARTITION BY source_asset_name, source_campaign_id, COALESCE(unknown_send_date, orig_send_date) ORDER BY orig_send_date DESC) AS rank
           			   |FROM
           			   |(
           			   |SELECT
           			   |      a.send_date AS orig_send_date
           			   |     ,b.send_date AS unknown_send_date
           			   |     ,COALESCE(a.source_asset_name, b.source_asset_name) AS source_asset_name
           			   |     ,COALESCE(a.source_campaign_id, b.source_campaign_id) AS source_campaign_id
           			   |     ,COALESCE(a.send_count, 0) AS orig_send_count
           			   |     ,COALESCE(a.unique_open_count, 0) AS orig_unique_open_count
           			   |     ,COALESCE(a.unique_click_count, 0) AS orig_unique_click_count
           			   |     ,COALESCE(a.bounce_count, 0) AS orig_bounce_count
           			   |     ,COALESCE(a.subscribe_count, 0) AS orig_subscribe_count
           			   |     ,COALESCE(a.unsubscribe_count, 0) AS orig_unsubscribe_count
           			   |     ,COALESCE(a.form_submit_count, 0) AS orig_form_submit_count
           			   |     ,COALESCE(a.total_open_count, 0) AS orig_total_open_count
           			   |     ,COALESCE(a.total_click_count, 0) AS orig_total_click_count
           			   |     ,COALESCE(b.send_count, 0) AS unknown_send_count
           			   |     ,COALESCE(b.unique_open_count, 0) AS unknown_unique_open_count
           			   |     ,COALESCE(b.unique_click_count, 0) AS unknown_unique_click_count
           			   |     ,COALESCE(b.bounce_count, 0) AS unknown_bounce_count
           			   |     ,COALESCE(b.subscribe_count, 0) AS unknown_subscribe_count
           			   |     ,COALESCE(b.unsubscribe_count, 0) AS unknown_unsubscribe_count
           			   |     ,COALESCE(b.form_submit_count, 0) AS unknown_form_submit_count
           			   |     ,COALESCE(b.total_open_count, 0) AS unknown_total_open_count
           			   |     ,COALESCE(b.total_click_count, 0) AS unknown_total_click_count
           			   |FROM
           			   |(SELECT * FROM marketingActivityCampaignSendFact WHERE send_date != ${conf("defaultDate")}) a
           			   |FULL OUTER JOIN
           			   |(SELECT * FROM marketingActivityCampaignSendFact WHERE send_date = ${conf("defaultDate")}) b
           			   |ON a.source_asset_name = b.source_asset_name AND a.source_campaign_id = b.source_campaign_id
           			   |)
           			   |)
           			   |WHERE rank = 1
				""".stripMargin


      log.info(s"Running Query: $query")
      val marketingActivityCampaignSendFact_tmp = sql(query).coalesce(1)
      marketingActivityCampaignSendFact_tmp.createOrReplaceTempView("marketingActivityCampaignSendFact_tmp")


      query =
        s"""
           			   |SELECT
           			   |      CAST(send_date AS Int) AS send_date
           			   |     ,source_asset_name
           			   |     ,source_campaign_id
           			   |     ,send_count
           			   |     ,unique_open_count
           			   |     ,unique_click_count
           			   |     ,bounce_count
           			   |     ,subscribe_count
           			   |     ,unsubscribe_count
           			   |     ,form_submit_count
           			   |     ,total_open_count
           			   |     ,total_click_count
           			   |FROM marketingActivityCampaignSendFact_tmp
           			   |UNION ALL
           			   |(
           			   |SELECT
           			   |      CAST(a.send_date AS Int) AS send_date
           			   |     ,a.source_asset_name
           			   |     ,a.source_campaign_id
           			   |     ,a.send_count
           			   |     ,a.unique_open_count
           			   |     ,a.unique_click_count
           			   |     ,a.bounce_count
           			   |     ,a.subscribe_count
           			   |     ,a.unsubscribe_count
           			   |     ,a.form_submit_count
           			   |     ,a.total_open_count
           			   |     ,a.total_click_count
           			   |FROM
           			   |(SELECT * FROM marketingActivityCampaignSendFact WHERE send_date != ${conf("defaultDate")}) a
           			   |LEFT JOIN marketingActivityCampaignSendFact_tmp b
           			   |ON a.send_date = b.send_date AND a.source_asset_name = b.source_asset_name AND a.source_campaign_id = b.source_campaign_id
           			   |WHERE b.send_date IS NULL AND b.source_asset_name IS NULL AND b.source_campaign_id IS NULL
           			   |)
				""".stripMargin

      log.info(s"Running Query: $query")
      val marketingActivityCampaignSendFact_preserved = sql(query).distinct.coalesce(1)
      marketingActivityCampaignSendFact_preserved.createOrReplaceTempView("marketingActivityCampaignSendFact_preserved")


      log.info("Attributing Unknown Data on source_campaign_id")

      query =
        s"""
           			   |SELECT
           			   |      send_date
           			   |     ,source_asset_name
           			   |     ,source_campaign_id
           			   |     ,send_count
           			   |     ,unique_open_count
           			   |     ,unique_click_count
           			   |     ,bounce_count
           			   |     ,subscribe_count
           			   |     ,unsubscribe_count
           			   |     ,form_submit_count
           			   |     ,total_open_count
           			   |     ,total_click_count
           			   |FROM
           			   |(
           			   |SELECT
           			   |      COALESCE(orig_send_date, unknown_send_date) AS send_date
           			   |     ,source_asset_name
           			   |     ,source_campaign_id
           			   |     ,orig_send_count + unknown_send_count AS send_count
           			   |     ,orig_unique_open_count + unknown_unique_open_count AS unique_open_count
           			   |     ,orig_unique_click_count + unknown_unique_click_count AS unique_click_count
           			   |     ,orig_bounce_count + unknown_bounce_count AS bounce_count
           			   |     ,orig_subscribe_count + unknown_subscribe_count AS subscribe_count
           			   |     ,orig_unsubscribe_count + unknown_unsubscribe_count AS unsubscribe_count
           			   |     ,orig_form_submit_count + unknown_form_submit_count AS form_submit_count
           			   |     ,orig_total_open_count + unknown_total_open_count AS total_open_count
           			   |     ,orig_total_click_count + unknown_total_click_count AS total_click_count
           			   |     ,row_number() OVER (PARTITION BY source_campaign_id, COALESCE(unknown_send_date, orig_send_date) ORDER BY orig_send_date DESC) AS rank
           			   |FROM
           			   |(
           			   |SELECT
           			   |      a.send_date AS orig_send_date
           			   |     ,b.send_date AS unknown_send_date
           			   |     ,COALESCE(a.source_asset_name, b.source_asset_name) AS source_asset_name
           			   |     ,COALESCE(a.source_campaign_id, b.source_campaign_id) AS source_campaign_id
           			   |     ,COALESCE(a.send_count, 0) AS orig_send_count
           			   |     ,COALESCE(a.unique_open_count, 0) AS orig_unique_open_count
           			   |     ,COALESCE(a.unique_click_count, 0) AS orig_unique_click_count
           			   |     ,COALESCE(a.bounce_count, 0) AS orig_bounce_count
           			   |     ,COALESCE(a.subscribe_count, 0) AS orig_subscribe_count
           			   |     ,COALESCE(a.unsubscribe_count, 0) AS orig_unsubscribe_count
           			   |     ,COALESCE(a.form_submit_count, 0) AS orig_form_submit_count
           			   |     ,COALESCE(a.total_open_count, 0) AS orig_total_open_count
           			   |     ,COALESCE(a.total_click_count, 0) AS orig_total_click_count
           			   |     ,COALESCE(b.send_count, 0) AS unknown_send_count
           			   |     ,COALESCE(b.unique_open_count, 0) AS unknown_unique_open_count
           			   |     ,COALESCE(b.unique_click_count, 0) AS unknown_unique_click_count
           			   |     ,COALESCE(b.bounce_count, 0) AS unknown_bounce_count
           			   |     ,COALESCE(b.subscribe_count, 0) AS unknown_subscribe_count
           			   |     ,COALESCE(b.unsubscribe_count, 0) AS unknown_unsubscribe_count
           			   |     ,COALESCE(b.form_submit_count, 0) AS unknown_form_submit_count
           			   |     ,COALESCE(b.total_open_count, 0) AS unknown_total_open_count
           			   |     ,COALESCE(b.total_click_count, 0) AS unknown_total_click_count
           			   |FROM
           			   |(SELECT * FROM marketingActivityCampaignSendFact_preserved WHERE send_date != ${conf("defaultDate")}) a
           			   |FULL OUTER JOIN
           			   |(
           			   |SELECT
           			   |      send_date
           			   |     ,source_campaign_id
           			   |     ,MAX(source_asset_name) AS source_asset_name
           			   |     ,SUM(send_count) AS send_count
           			   |     ,SUM(unique_open_count) AS unique_open_count
           			   |     ,SUM(unique_click_count) AS unique_click_count
           			   |     ,SUM(bounce_count) AS bounce_count
           			   |     ,SUM(subscribe_count) AS subscribe_count
           			   |     ,SUM(unsubscribe_count) AS unsubscribe_count
           			   |     ,SUM(form_submit_count) AS form_submit_count
           			   |     ,SUM(total_open_count) AS total_open_count
           			   |     ,SUM(total_click_count) AS total_click_count
           			   |FROM marketingActivityCampaignSendFact_preserved
           			   |WHERE send_date = ${conf("defaultDate")}
           			   |GROUP BY 1,2
           			   |) b
           			   |ON a.source_campaign_id = b.source_campaign_id
           			   |)
           			   |)
           			   |WHERE rank = 1
				""".stripMargin


      log.info(s"Running Query: $query")
      val marketingActivityCampaignSendFactStage = sql(query).coalesce(1)
      marketingActivityCampaignSendFactStage.createOrReplaceTempView("marketingActivityCampaignSendFactStage")


      query =
        s"""
           			   |SELECT
           			   |      CAST(send_date AS Int) AS send_date
           			   |     ,source_asset_name
           			   |     ,source_campaign_id
           			   |     ,send_count
           			   |     ,unique_open_count
           			   |     ,unique_click_count
           			   |     ,bounce_count
           			   |     ,subscribe_count
           			   |     ,unsubscribe_count
           			   |     ,form_submit_count
           			   |     ,total_open_count
           			   |     ,total_click_count
           			   |FROM marketingActivityCampaignSendFactStage
           			   |UNION ALL
           			   |(
           			   |SELECT
           			   |      CAST(a.send_date AS Int) AS send_date
           			   |     ,a.source_asset_name
           			   |     ,a.source_campaign_id
           			   |     ,a.send_count
           			   |     ,a.unique_open_count
           			   |     ,a.unique_click_count
           			   |     ,a.bounce_count
           			   |     ,a.subscribe_count
           			   |     ,a.unsubscribe_count
           			   |     ,a.form_submit_count
           			   |     ,a.total_open_count
           			   |     ,a.total_click_count
           			   |FROM
           			   |(SELECT * FROM marketingActivityCampaignSendFact_preserved WHERE send_date != ${conf("defaultDate")}) a
           			   |LEFT JOIN marketingActivityCampaignSendFactStage b
           			   |ON a.send_date = b.send_date AND a.source_asset_name = b.source_asset_name AND a.source_campaign_id = b.source_campaign_id
           			   |WHERE b.send_date IS NULL AND b.source_asset_name IS NULL AND b.source_campaign_id IS NULL
           			   |)
				""".stripMargin

      log.info(s"Running Query: $query")
      val marketingActivityCampaignSendFactFinal = sql(query).distinct.coalesce(1)

      marketingActivityCampaignSendFactFinal.createOrReplaceTempView("marketingActivityCampaignSendFactFinal")
      marketingActivityCampaignSendFactFinal.persist()
      marketingActivityCampaignSendFactFinal.count()


      log.info("Validation")
      query =
        """
          |SELECT
          |      'send_fact_metrics' AS table
          |     ,CAST(SUM(send_count) AS string) AS send_count
          |     ,CAST(SUM(unique_open_count) AS string) AS unique_open_count
          |     ,CAST(SUM(unique_click_count) AS string) AS unique_click_count
          |     ,CAST(SUM(bounce_count) AS string) AS bounce_count
          |     ,CAST(SUM(subscribe_count) AS string) AS subscribe_count
          |     ,CAST(SUM(unsubscribe_count) AS string) AS unsubscribe_count
          |     ,CAST(SUM(form_submit_count) AS string) AS form_submit_count
          |     ,CAST(SUM(total_open_count) AS string) AS total_open_count
          |     ,CAST(SUM(total_click_count) AS string) AS total_click_count
          |FROM marketingActivityCampaignSendFact
        """.stripMargin

      log.info(s"Running Query: $query")
      val send_fact = sql(query)
      send_fact.persist
      send_fact.show(false)

      query =
        """
          |SELECT
          |      'send_fact_attr_metrics' AS table
          |     ,CAST(SUM(send_count) AS string) AS send_count
          |     ,CAST(SUM(unique_open_count) AS string) AS unique_open_count
          |     ,CAST(SUM(unique_click_count) AS string) AS unique_click_count
          |     ,CAST(SUM(bounce_count) AS string) AS bounce_count
          |     ,CAST(SUM(subscribe_count) AS string) AS subscribe_count
          |     ,CAST(SUM(unsubscribe_count) AS string) AS unsubscribe_count
          |     ,CAST(SUM(form_submit_count) AS string) AS form_submit_count
          |     ,CAST(SUM(total_open_count) AS string) AS total_open_count
          |     ,CAST(SUM(total_click_count) AS string) AS total_click_count
          |FROM marketingActivityCampaignSendFactFinal
        """.stripMargin

      log.info(s"Running Query: $query")
      val send_fact_attr = sql(query)
      send_fact_attr.persist
      send_fact_attr.show(false)


      val send_fact_metrics = transposeUDF(send_fact, Seq("table"))
      send_fact_metrics.createOrReplaceTempView("send_fact_metrics")

      val send_fact_attr_metrics = transposeUDF(send_fact_attr, Seq("table"))
      send_fact_attr_metrics.createOrReplaceTempView("send_fact_attr_metrics")

      query =
        s"""
           |SELECT
           |      a.column_name AS metric
           |     ,a.column_value AS send_fact
           |     ,b.column_value AS send_fact_attr
           |     ,(CAST(a.column_value AS LONG) - CAST(b.column_value AS LONG)) AS ${conf("thresholdColumn")}
           |FROM send_fact_metrics a JOIN send_fact_attr_metrics b
           |ON a.column_name = b.column_name
           |WHERE ABS(a.column_value - b.column_value) > ${conf("dc.errorThreshold")}
        """.stripMargin
      log.info(s"Running Query: $query")
      val df = sql(query)
      df.show(false)
      df.columns


      if (df.count > 0) {
        val htmlBody = df.collect.map { row =>
          val value = row.getAs(
            conf("thresholdColumn")).asInstanceOf[Long].abs; (if (value > conf("dc.errorThreshold").toInt) s"<tr bgcolor=${conf("red")}><td>") + row.toString.stripPrefix("[").stripSuffix("]").split(",").mkString("</td><td>") + "</td></tr>"
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
             |    <th>Send_Fact</th>
             |    <th>Send_Fact_Attr</th>
             |    <th>Diff</th>
             |  </tr>
             |  $htmlBody
             |</table>
           """.stripMargin

        send a Mail(
          from = conf("from") -> conf("fromName"),
          to = conf("to").split(","),
          subject = conf("dc.subject"),
          message = conf("dc.message"),
          richMessage = Option(rMsg)
        )


      }


      log.info(s"Saving Data to target Location: $finalDataLocation")
      saveDataFrameToDisk(marketingActivityCampaignSendFactFinal, SaveMode.Overwrite, targetDataFormat, finalDataLocation)

      log.info("Check and Create Hive DDL")
      checkAndCreateHiveDDL(hiveMetaStore, targetDB, finalTable, targetDataFormat, finalDataLocation, getColsFromDF(marketingActivityCampaignSendFactFinal))

      jc.logInfo(campaignInstanceId, "END Daily Campaign sendFact Process", -1)

      jc.endJob(campaignInstanceId, 1, startTimestamp, endTimestamp)

      jc.endJob(instanceId, 1)
    }
    catch {
      case e: Throwable  =>
        log.info(s"Something went WRONG during the DailyCampaignSendFact run for Instance: $campaignInstanceId")
        jc.endJob(campaignInstanceId, -1)
        throw e
    }

  }


}
