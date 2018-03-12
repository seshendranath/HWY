package com.homeaway.analyticsengineering.task

/**
  * Created by aguyyala on 10/19/17.
  */


import java.text.SimpleDateFormat

import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.{DeleteObjectsRequest, ListObjectsV2Request, ListObjectsV2Result}
import com.github.nscala_time.time.Imports._
import com.homeaway.analyticsengineering.AnalyticsTaskApp._
import com.homeaway.analyticsengineering.encrypt.main.utilities.Email.{Mail, send}
import com.homeaway.analyticsengineering.encrypt.main.utilities.Utility
import org.apache.hadoop.fs.Path
import org.apache.hadoop.tools.{DistCp, DistCpOptions}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer


class DailyEmailSendFact_Load extends Utility {

  def run(): Unit = {

    val jobId = jc.getJobId(conf("de.jobName"), conf("de.objectName"), conf("de.jobName"))
    val (emailInstanceId, emailLastSuccessfulRunDetails) = jc.startJob(jobId)


    val sourceDataLocation = conf("s3aUri") + conf("omniBucket") + "/" + conf("de.omniPrefix")
    val sourceDataFormat = conf("sourceDataFormat")
    val targetDataFormat = conf("targetDataFormat")
    val targetDataLocation = conf("s3aUri") + conf("s3Bucket") + "/" + conf("aeTier1DB")+ "/"  + conf("de.targetDataLocationPrefix")
    val tmpString = conf("tmpString")
    val targetDataTmpLocation = conf("hdfsLoc") + "/" + conf("aeTier1DB")+ "/" + conf("de.targetDataLocationPrefix") + tmpString
    val targetDataUnknownTmpLocation = conf("hdfsLoc") + "/" + conf("aeTier1DB")+ "/" +conf("de.targetDataLocationPrefix") + "_unknown" + tmpString
    val targetDB = conf("aeTier1DB")
    val targetTable = conf("de.objectName")
    val offset = conf("de.offset").toInt
    val targetPartCols = Array("send_date")
    val targetUnknownDataLocation = targetDataLocation + "/" + targetPartCols.head + "=" + conf("defaultDate")

    val sql = spark.sql _

    val timeFormat = "yyyy-MM-dd HH:mm:ss"
    val timeFormatter = new SimpleDateFormat(timeFormat)

    val dateFormat = "yyyyMMdd"
    val dateFormatter = DateTimeFormat.forPattern(dateFormat)

    val endDate = (DateTime.now - 1.day).toString(dateFormat)
    val startDate = DateTime.parse(endDate, dateFormatter).minusDays(offset).toString(dateFormat)

    val defaultStartTimestamp = (DateTime.now - 1.day).toString(timeFormat)
    val startTimestamp = emailLastSuccessfulRunDetails.getOrElse("lastSuccessfulWaterMarkEndTime", defaultStartTimestamp)
    val endTimestamp = DateTime.now.toString(timeFormat)


    try {

      jc.logInfo(instanceId, s"START Daily Email sendFact Process", -1)

      log.info(s"Reading all Modified Directories in $sourceDataLocation from the last timestamp $startTimestamp")

      val s3 = AmazonS3ClientBuilder.defaultClient()
      val req = new ListObjectsV2Request().withBucketName(conf("omniBucket")).withPrefix(conf("de.omniPrefix")+"/")
      var result = new ListObjectsV2Result
      val ftp = new ArrayBuffer[String]()
      val dtPattern = """dateid=\d{8}""".r

      do {
        result = s3.listObjectsV2(req)
        ftp ++= result.getObjectSummaries.filter { x => val modifiedTime = timeFormatter.format(x.getLastModified.getTime); modifiedTime > startTimestamp && modifiedTime <= endTimestamp && !Set("success", "folder").exists(e => x.getKey.toLowerCase.matches(s".*$e.*")) }.map(x => conf("s3aUri") + conf("omniBucket") + "/" + x.getKey)
        req.setContinuationToken(result.getNextContinuationToken)
      } while (result.isTruncated)

      val datesToProcess = ftp.map(dtPattern.findFirstIn(_).getOrElse(None)).distinct.filterNot(_ == None)
      val filesToProcess = ftp.toArray

      log.info(s"The following dates ${datesToProcess.mkString(",")} got modified from the last run at $startTimestamp")
      if (datesToProcess.isEmpty || filesToProcess.isEmpty) {
        log.info("No Dates to Process. Either the threshold is too high or the Upstream didn't write any files")
        log.info(s"Updating Job Instance $instanceId with Status: SUCCEEDED, StartTime: $startTimestamp, EndTime: $endTimestamp")
        jc.logInfo(instanceId, s"Updating Job Instance $instanceId with Status: SUCCEEDED, StartTime: $startTimestamp, EndTime: $endTimestamp", -1)
        jc.endJob(instanceId, 1, startTimestamp, endTimestamp)
        System.exit(0)
      }

      log.info(s"Reading Source Data from location $sourceDataLocation")
      val maeWhole = spark.read.format(sourceDataFormat).load(filesToProcess: _*).distinct
      val mae = maeWhole.where("sourceemailrecipientid IS NOT NULL AND sourceemailrecipientid != ''")
      mae.createOrReplaceTempView("mae")


      log.info("Reading unknown sourceemailrecipientids from source")
      val maeUnknown = maeWhole.where("sourceemailrecipientid IS NULL OR TRIM(sourceemailrecipientid) = ''")
      maeUnknown.createOrReplaceTempView("maeUnknown")

      log.info("Aggregating the Source Data by ActivityDate, AssetName and CampaignId")
      var query =
        """
          			  |SELECT
          			  |      sourceemailrecipientid
          			  |     ,CAST(date_format(CAST(activitydate AS DATE), "yyyyMMdd") AS INT) AS sendDate
          			  |     ,sourceassetname
          			  |     ,sourcecampaignid
          			  |FROM mae
          			  |WHERE activitytype = 'EmailSend'
          			  |GROUP BY 1, 2, 3, 4
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
          			  |     ,MIN(CASE WHEN activitytype='EmailOpen' THEN CAST(date_format(CAST(activitydate AS DATE), "yyyyMMdd") AS INT) ELSE NULL END) AS firstOpenDate
          			  |     ,MIN(CASE WHEN activitytype='EmailClickthrough' THEN CAST(date_format(CAST(activitydate AS DATE), "yyyyMMdd") AS INT) ELSE NULL END) AS firstClickDate
          			  |     ,MIN(CASE WHEN activitytype='Bounceback' THEN CAST(date_format(CAST(activitydate AS DATE), "yyyyMMdd") AS INT) ELSE NULL END) AS firstBounceDate
          			  |     ,MIN(CASE WHEN activitytype='Subscribe' THEN CAST(date_format(CAST(activitydate AS DATE), "yyyyMMdd") AS INT) ELSE NULL END) AS firstSubscribeDate
          			  |     ,MIN(CASE WHEN activitytype='Unsubscribe' THEN CAST(date_format(CAST(activitydate AS DATE), "yyyyMMdd") AS INT) ELSE NULL END) AS firstUnsubscribeDate
          			  |     ,MIN(CASE WHEN activitytype='FormSubmit' THEN CAST(date_format(CAST(activitydate AS DATE), "yyyyMMdd") AS INT) ELSE NULL END) AS firstFormSubmitDate
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

      log.info("Building Stage Marketing Activity Email SendFact Delta Table")
      query =
        """
          			  |SELECT
          			  |       COALESCE(a.sourceemailrecipientid, b.sourceemailrecipientid) AS source_email_recipient_id
          			  |      ,COALESCE(a.sourceassetname, b.sourceassetname) AS source_asset_name
          			  |      ,COALESCE(a.sourcecampaignid, b.sourcecampaignid) AS source_campaign_id
          			  |      ,sendDate AS send_date
          			  |      ,firstOpenDate AS first_open_date
          			  |      ,firstClickDate AS first_click_date
          			  |      ,firstBounceDate AS first_bounce_date
          			  |      ,firstSubscribeDate AS first_subscribe_date
          			  |      ,firstUnsubscribeDate AS first_unsubscribe_date
          			  |      ,firstFormSubmitDate AS first_form_submit_date
          			  |      ,COALESCE(EmailOpen_count, 0) AS email_open_count
          			  |      ,COALESCE(EmailClickthrough_count, 0) AS email_click_count
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
        			""".stripMargin

      log.info(s"Running Query: $query")
      val marketingActivityEmailSendFactStage = sql(query)
      marketingActivityEmailSendFactStage.createOrReplaceTempView("marketingActivityEmailSendFactStage")


      query =
        s"""
           			   |SELECT *
           			   |FROM
           			   |(
           			   |SELECT
           			   |      COALESCE(a.source_email_recipient_id, b.source_email_recipient_id) AS source_email_recipient_id
           			   |     ,COALESCE(a.source_asset_name, b.source_asset_name) AS source_asset_name
           			   |     ,COALESCE(a.source_campaign_id, b.source_campaign_id) AS source_campaign_id
           			   |     ,LEAST(a.first_open_date, b.first_open_date) AS first_open_date
           			   |     ,LEAST(a.first_click_date, b.first_click_date) AS first_click_date
           			   |     ,LEAST(a.first_bounce_date, b.first_bounce_date) AS first_bounce_date
           			   |     ,LEAST(a.first_subscribe_date, b.first_subscribe_date) AS first_subscribe_date
           			   |     ,LEAST(a.first_unsubscribe_date, b.first_unsubscribe_date) AS first_unsubscribe_date
           			   |     ,LEAST(a.first_form_submit_date, b.first_form_submit_date) AS first_form_submit_date
           			   |     ,CAST(COALESCE(a.email_open_count, 0) + COALESCE(b.email_open_count, 0) AS Int) AS email_open_count
           			   |     ,CAST(COALESCE(a.email_click_count, 0) + COALESCE(b.email_click_count, 0) AS Int) AS email_click_count
           			   |     ,COALESCE(a.send_date, b.send_date) AS send_date
           			   |FROM
           			   |(SELECT *
           			   |FROM $targetDB.$targetTable
           			   |WHERE send_date BETWEEN $startDate AND $endDate
           			   |) a
           			   |FULL OUTER JOIN marketingActivityEmailSendFactStage b
           			   |ON a.source_email_recipient_id = b.source_email_recipient_id
           			   |AND a.source_asset_name = b.source_asset_name
           			   |AND a.source_campaign_id = b.source_campaign_id
           			   |)
                  """.stripMargin

      log.info(s"Running Query: $query")
      val marketingActivityEmailSendFactTMP = sql(query)
      marketingActivityEmailSendFactTMP.persist

      log.info(s"Saving Final Unknown Data to TEMP target Location: $targetDataUnknownTmpLocation")
      saveDataFrameToDisk(marketingActivityEmailSendFactTMP.where("send_date IS NULL"), SaveMode.Overwrite, targetDataFormat, targetDataUnknownTmpLocation)


      val marketingActivityEmailSendFactKnown = marketingActivityEmailSendFactTMP.where("send_date IS NOT NULL")
      val marketingActivityEmailSendFact = marketingActivityEmailSendFactKnown.repartition(targetPartCols.map(x => col(x)): _*)

      log.info(s"Saving Data to TEMP target Location: $targetDataTmpLocation")
      saveDataFrameToDisk(marketingActivityEmailSendFact, SaveMode.Overwrite, targetDataFormat, targetDataTmpLocation, targetPartCols)

      val dataTransferStartTime = System.nanoTime()

      val tmpDirs = dfs.globStatus(new Path(targetDataTmpLocation + "/*" * targetPartCols.length)).map(fs => fs.getPath.toString).filterNot(_ contains "_SUCCESS")

      val keys = new ArrayBuffer[String]()
      for (i <- tmpDirs) {
        val Array(_, partition) = i.split(tmpString)
        val dir = conf("aeTier1DB") + "/" + conf("de.targetDataLocationPrefix") + partition
        log.info(s"Deleting Existing Data ${conf("s3Bucket")}/$dir")
        val req = new ListObjectsV2Request().withBucketName(conf("s3Bucket")).withPrefix(dir+"/")
        var result = new ListObjectsV2Result

        do {
          result = s3.listObjectsV2(req)
          keys ++= result.getObjectSummaries.map(x => x.getKey)
          req.setContinuationToken(result.getNextContinuationToken)
        } while (result.isTruncated)

      }


      log.info(s"Distcp Data from $targetDataTmpLocation to $targetDataLocation")
      val distCpOptions = new DistCpOptions(dfs.globStatus(new Path(targetDataTmpLocation)).map(_.getPath).toList, new Path(targetDataLocation + "/"))
      distCpOptions.setOverwrite(true)
      distCpOptions.setMaxMaps(conf("de.distcpNumMappers").toInt)

      new DistCp(spark.sparkContext.hadoopConfiguration, distCpOptions).execute()

      val dataTransferEndTime = System.nanoTime()
      val totalTime = (dataTransferEndTime - dataTransferStartTime) / (1e9 * 60)
      log.info("Total Elapsed time to transfer Data from Stage to Final S3 location: " + f"$totalTime%2.2f" + " mins")

      val exclude = dfs.globStatus(new Path(targetDataTmpLocation + "/*/*")).map(_.getPath).map(_.toString).map(_.split("/").takeRight(2).mkString("/")).toSet
      val deleteKeys = keys.filterNot(key => exclude.exists(key contains _))
      if (deleteKeys.nonEmpty) {
        val multiObjectDeleteRequest = new DeleteObjectsRequest(conf("s3Bucket")).withKeys(deleteKeys: _*)
        s3.deleteObjects(multiObjectDeleteRequest)
      }

      val marketingActivityEmailSendFactUnknown = spark.read.format(targetDataFormat).load(targetDataUnknownTmpLocation)
      marketingActivityEmailSendFactUnknown.createOrReplaceTempView("marketingActivityEmailSendFactUnknown")

      query =
        s"""
           			   |SELECT
           			   |      CONCAT(emailaddress, sourceactivityid) AS source_email_recipient_id
           			   |     ,sourceassetname AS source_asset_name
           			   |     ,sourcecampaignid AS source_campaign_id
           			   |     ,MIN(CASE WHEN activitytype='EmailOpen' THEN CAST(date_format(CAST(activitydate AS DATE), "yyyyMMdd") AS INT) ELSE NULL END) AS first_open_date
           			   |     ,MIN(CASE WHEN activitytype='EmailClickthrough' THEN CAST(date_format(CAST(activitydate AS DATE), "yyyyMMdd") AS INT) ELSE NULL END) AS first_click_date
           			   |     ,MIN(CASE WHEN activitytype='Bounceback' THEN CAST(date_format(CAST(activitydate AS DATE), "yyyyMMdd") AS INT) ELSE NULL END) AS first_bounce_date
           			   |     ,MIN(CASE WHEN activitytype='Subscribe' THEN CAST(date_format(CAST(activitydate AS DATE), "yyyyMMdd") AS INT) ELSE NULL END) AS first_subscribe_date
           			   |     ,MIN(CASE WHEN activitytype='Unsubscribe' THEN CAST(date_format(CAST(activitydate AS DATE), "yyyyMMdd") AS INT) ELSE NULL END) AS first_unsubscribe_date
           			   |     ,MIN(CASE WHEN activitytype='FormSubmit' THEN CAST(date_format(CAST(activitydate AS DATE), "yyyyMMdd") AS INT) ELSE NULL END) AS first_form_submit_date
           			   |     ,CAST(SUM(CASE WHEN activitytype='EmailOpen' THEN 1 ELSE 0 END) AS INT) AS email_open_count
           			   |     ,CAST(SUM(CASE WHEN activitytype='EmailClickthrough' THEN 1 ELSE 0 END) AS INT) AS email_click_count
           			   |FROM maeUnknown
           			   |GROUP BY 1, 2, 3
           			   |UNION ALL
           			   |SELECT
           			   |      source_email_recipient_id
           			   |     ,source_asset_name
           			   |     ,source_campaign_id
           			   |     ,first_open_date
           			   |     ,first_click_date
           			   |     ,first_bounce_date
           			   |     ,first_subscribe_date
           			   |     ,first_unsubscribe_date
           			   |     ,first_form_submit_date
           			   |     ,email_open_count
           			   |     ,email_click_count
           			   |FROM marketingActivityEmailSendFactUnknown
				""".stripMargin

      log.info(s"Running Query: $query")
      val unknown = sql(query).coalesce(1)
      unknown.createOrReplaceTempView("unknown")

      log.info(s"""Saving unknown Data to target Location:  $targetUnknownDataLocation""")
      saveDataFrameToDisk(unknown, SaveMode.Append, targetDataFormat, targetUnknownDataLocation)

      marketingActivityEmailSendFactTMP.unpersist

      log.info("Check and Create Hive DDL")
      checkAndCreateHiveDDL(hiveMetaStore, targetDB, targetTable, targetDataFormat, targetDataLocation, getColsFromDF(marketingActivityEmailSendFact, targetPartCols), targetPartCols.map((_, "string")))

      val partitions = tmpDirs.map(_.split(tmpString + "/").last).map { x => (List(x.split("=").last), targetDataLocation + "/" + x) }.toList

      log.info("Add Hive Partitions")
      addHivePartitions(hiveMetaStore, targetDB, targetTable, partitions)


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
           |WHERE ABS(ROUND(((CAST(a.column_value AS Double) - CAST(b.column_value AS Double))/CAST(a.column_value AS Double)) * 100, 2)) > ${conf("de.errorThreshold")}
        """.stripMargin
      log.info(s"Running Query: $query")
      val df = sql(query)
      df.show(false)
      df.columns


      if (df.count > 0) {
        val htmlBody = df.collect.map { row =>
          val value = row.getAs(conf("thresholdColumn")).asInstanceOf[Double].abs; (if (value > conf("de.errorThreshold").toInt) s"<tr bgcolor=${conf("red")}><td>") + row.toString.stripPrefix("[").stripSuffix("]").split(",").mkString("</td><td>") + "</td></tr>"
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
          subject = conf("de.subject"),
          message = conf("de.message"),
          richMessage = Option(rMsg)
        )


      }


      jc.logInfo(emailInstanceId, "END Daily Email sendFact Process", -1)

      jc.endJob(emailInstanceId, 1, startTimestamp, endTimestamp)
      jc.endJob(instanceId, 1)

    }
    catch {
      case e: Throwable  =>
        log.info(s"Something went WRONG during the DailyEmailSendFact run for Instance: $emailInstanceId")
        jc.endJob(emailInstanceId, -1)
        throw e
    }
  }

}