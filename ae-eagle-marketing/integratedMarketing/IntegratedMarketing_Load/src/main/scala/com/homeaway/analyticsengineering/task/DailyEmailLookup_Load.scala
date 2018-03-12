package com.homeaway.analyticsengineering.task


import java.text.SimpleDateFormat
import com.amazonaws.services.s3.model.{ListObjectsV2Request, ListObjectsV2Result}
import com.homeaway.analyticsengineering.AnalyticsTaskApp._
import com.homeaway.analyticsengineering.encrypt.main.utilities.Utility
import org.apache.spark.sql.SaveMode
import org.joda.time.DateTime
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer


class DailyEmailLookup_Load extends Utility {


  def run(): Unit = {

    val jobId = jc.getJobId(conf("el.dailyJobName"), conf("el.objectName"), conf("el.dailyJobName"))
    val (emailInstanceId, emailLastSuccessfulRunDetails) = jc.startJob(jobId)

    val timeFormat = "yyyy-MM-dd HH:mm:ss"
    val timeFormatter = new SimpleDateFormat(timeFormat)
    val defaultStartTimestamp = DateTime.now.minusDays(1).toString(timeFormat)
    val startTimestamp = emailLastSuccessfulRunDetails.getOrElse("lastSuccessfulWaterMarkEndTime", defaultStartTimestamp)
    val endTimestamp = DateTime.now.toString(timeFormat)


    val s3aUri = conf("s3aUri")
    val omniBucket = conf("omniBucket")
    val sourcePrefix = conf("omniDataLocationPrefix") + "/" + conf("el.sourceDataLocationPrefix")
    val sourceDataFormat = conf("sourceDataFormat")
    val datePattern = """dateid=\d{8}""".r
    val campaignSourceDataLocation = conf("s3aUri") + conf("omniBucket") + "/" + conf("omniDataLocationPrefix") + "/" + conf("campaignSourceDataLocationPrefix")
    val targetDB = conf("aeStageDB")
    val targetTable = conf("el.objectName")
    val targetDataFormat = conf("targetDataFormat")
    val targetDataTmpLocation = conf("hdfsLoc") + "/" + conf("targetDataLocationPrefix") + "/" + conf("el.objectName") + conf("tmpString")
    val targetDataLocation = conf("s3aUri") + conf("s3StageBucket") + "/" + conf("targetDataLocationPrefix") + "/" + conf("el.objectName")


    val sql = spark.sql _
    import spark.implicits._


    try {

      jc.logInfo(emailInstanceId, s"START DailyEmailLookup_Load Process", -1)

      log.info(s"Reading all Modified Directories in source data location from the last timestamp $startTimestamp")

      val ftp = new ArrayBuffer[String]()
      val req = new ListObjectsV2Request().withBucketName(omniBucket).withPrefix(sourcePrefix+"/")
      var result = new ListObjectsV2Result
      do {
        result = s3.listObjectsV2(req)
        ftp ++= result.getObjectSummaries.filter { x => val modifiedTime = timeFormatter.format(x.getLastModified.getTime); modifiedTime > startTimestamp && modifiedTime <= endTimestamp && !Set("success", "folder").exists(e => x.getKey.toLowerCase.matches(s".*$e.*")) }.map(x => s3aUri + omniBucket + "/" + x.getKey)
        req.setContinuationToken(result.getNextContinuationToken)
      } while (result.isTruncated)

      ftp.foreach { a => println(a) }

      val datesToProcess = ftp.map(datePattern.findFirstIn(_).getOrElse(None)).distinct.filterNot(_ == None)
      val filesToProcess = ftp.toArray

      log.info(s"The following dates ${datesToProcess.mkString(",")} got modified from the last run at $startTimestamp")
      if (datesToProcess.isEmpty || filesToProcess.isEmpty) {
        log.info("No Dates to Process. Either the threshold is too high or the Upstream didn't write any files")
        log.info(s"Updating Job Instance $emailInstanceId with Status: SUCCEEDED, StartTime: $startTimestamp, EndTime: $endTimestamp")
        jc.logInfo(emailInstanceId, s"Updating Job Instance $emailInstanceId with Status: SUCCEEDED, StartTime: $startTimestamp, EndTime: $endTimestamp", -1)
        jc.endJob(emailInstanceId, 1, startTimestamp, endTimestamp)
        jc.endJob(instanceId, 1)
        cleanup()
        System.exit(0)
      }


      log.info(s"Reading Source Data marketingActivityEloqua")
      val mae = spark.read.format(sourceDataFormat).load(filesToProcess: _*).select("activityDate", "activityType", "sourceAssetName", "sourceCampaignId", "sourceEmailRecipientId", "emailSendType", "sourceAssetType", "sourceActivityId").where($"activityType" === "EmailSend").distinct

      mae.persist
      mae.count
      mae.createOrReplaceTempView("mae")



      log.info(s"Reading marketingEloquaCampaigns from location $campaignSourceDataLocation")
      val mec = spark.read.parquet(campaignSourceDataLocation).select("campaignId", "campaignName")

      mec.persist
      mec.count
      mec.createOrReplaceTempView("mec")


      log.info("Join source with mec to retain only marketing emails")
      var query =
        """
          |SELECT
          |      a.sourceEmailRecipientId
          |     ,MAX(CAST(date_format(CAST(activitydate AS DATE), "yyyyMMdd") AS String)) AS activityDate
          |     ,MAX(a.sourceAssetName) AS sourceAssetName
          |     ,MAX(a.sourceAssetType) AS sourceAssetType
          |     ,MAX(a.sourceCampaignId) AS sourceCampaignId
          |     ,MAX(a.emailSendType) AS emailSendType
          |     ,MAX(b.campaignName) AS campaignName
          |FROM mae a
          |LEFT JOIN mec b
          |ON a.sourcecampaignId = b.campaignId
          |WHERE b.campaignId IS NOT NULL
          |GROUP BY 1
        """.stripMargin


      val emailLookupDF = sql(query).coalesce(conf("el.numPartitions").toInt)
      emailLookupDF.persist
      emailLookupDF.count
      emailLookupDF.createOrReplaceTempView("emailLookupDF")

      log.info("Building Email Lookup Daily Fact")
      query =
        s"""
           |SELECT
           |      COALESCE(b.sourceEmailRecipientId, a.sourceEmailRecipientId) AS sourceEmailRecipientId
           |     ,COALESCE(b.activityDate,a.activityDate) AS activityDate
           |     ,COALESCE(b.sourceAssetName,a.sourceAssetName) AS sourceAssetName
           |     ,COALESCE(b.sourceAssetType,a.sourceAssetType) AS sourceAssetType
           |     ,COALESCE(b.sourceCampaignId,a.sourceCampaignId) AS sourceCampaignId
           |     ,COALESCE(b.emailSendType,a.emailSendType) AS emailSendType
           |     ,COALESCE(b.campaignName,a.campaignName) AS campaignName
           |FROM $targetDB.$targetTable a
           |FULL OUTER JOIN emailLookupDF b
           |ON a.sourceEmailRecipientId = b.sourceEmailRecipientId
        """.stripMargin

      log.info(s"Running Query: $query")
      val rlt = sql(query)
      rlt.persist

      log.info(s"Saving Data to TEMP Target Location: $targetDataTmpLocation")
      saveDataFrameToDisk(rlt, SaveMode.Overwrite, targetDataFormat, targetDataTmpLocation)


      log.info(s"Read Data from TEMP Location: $targetDataTmpLocation")
      val final_metrics = spark.read.format(targetDataFormat).load(targetDataTmpLocation).coalesce(conf("el.numPartitions").toInt)

      log.info(s"Write Data to S3 Target Location: $targetDataLocation")
      saveDataFrameToDisk(final_metrics, SaveMode.Overwrite, targetDataFormat, targetDataLocation)

      log.info(s"Create Hive Table: $targetDB.$targetTable")
      checkAndCreateHiveDDL(hiveMetaStore, targetDB, targetTable, targetDataFormat, targetDataLocation, getColsFromDF(final_metrics))


      jc.logInfo(emailInstanceId, "END DailyEmailLookup_Load process", -1)


      jc.endJob(emailInstanceId, 1, startTimestamp, endTimestamp)
      jc.endJob(instanceId, 1)
    }
    catch {
      case e: Throwable  =>
        log.info(s"Something went WRONG during the DailyEmailLookup run for Instance: $emailInstanceId")
        jc.endJob(emailInstanceId, -1)
        throw e
    }


  }

}
