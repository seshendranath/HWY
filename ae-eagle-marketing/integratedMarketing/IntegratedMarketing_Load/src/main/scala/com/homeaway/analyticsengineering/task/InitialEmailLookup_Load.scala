package com.homeaway.analyticsengineering.task


import java.text.SimpleDateFormat
import java.util.Calendar
import com.homeaway.analyticsengineering.AnalyticsTaskApp._
import com.homeaway.analyticsengineering.encrypt.main.utilities.Utility
import org.apache.spark.sql.SaveMode

class InitialEmailLookup_Load extends Utility {

  def run(): Unit = {

    val jobId = jc.getJobId(conf("el.jobName"), conf("el.objectName"), conf("el.jobName"))
    val (emailInstanceId, _) = jc.startJob(jobId)

    val eloquaSourceDataLocation = conf("s3aUri") + conf("omniBucket") + "/" + conf("omniDataLocationPrefix") + "/" + conf("el.sourceDataLocationPrefix")
    val campaignSourceDataLocation = conf("s3aUri") + conf("omniBucket") + "/" + conf("omniDataLocationPrefix") + "/" + conf("campaignSourceDataLocationPrefix")
    val sourceDataFormat = conf("sourceDataFormat")
    val dailyJobName = conf("el.dailyJobName")
    val targetDataFormat = conf("targetDataFormat")
    val targetDataLocation = conf("s3aUri") + conf("s3StageBucket") + "/" + conf("targetDataLocationPrefix") + "/" + conf("el.objectName")
    val targetDB = conf("aeStageDB")
    val targetTable = conf("el.objectName")

    val timeFormat = "yyyy-MM-dd HH:mm:ss"
    val timeFormatter = new SimpleDateFormat(timeFormat)

    val cal = Calendar.getInstance()
    val startTimestamp = timeFormatter.format(cal.getTime)

    val sql = spark.sql _
    import spark.implicits._

    try {

      jc.logInfo(instanceId, s"START InitialEmailLookup_Load Process", -1)

      log.info(s"Reading Source Data marketingActivityEloqua from location $eloquaSourceDataLocation")
      val mae = spark.read.format(sourceDataFormat).load(eloquaSourceDataLocation).select("activityDate", "activityType", "sourceAssetName", "sourceCampaignId", "sourceEmailRecipientId", "emailSendType", "sourceAssetType", "sourceActivityId").where($"activityType" === "EmailSend").distinct

      mae.persist
      mae.count
      mae.createOrReplaceTempView("mae")


      log.info(s"Reading marketingEloquaCampaigns from location $campaignSourceDataLocation")
      val mec = spark.read.parquet(campaignSourceDataLocation).select("campaignId", "campaignName")

      mec.persist
      mec.count
      mec.createOrReplaceTempView("mec")

      log.info("Join source with mec to retain only marketing emails")
      val query =
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


      log.info(s"Saving Data to target Location: $targetDataLocation")
      saveDataFrameToDisk(emailLookupDF, SaveMode.Overwrite, targetDataFormat, targetDataLocation)

      log.info(s"Check and Create Hive Table: $targetDB.$targetTable")
      checkAndCreateHiveDDL(hiveMetaStore, targetDB, targetTable, targetDataFormat, targetDataLocation, getColsFromDF(emailLookupDF))

      jc.logInfo(instanceId, "END InitialEmailLookup_Load process", -1)

      log.info(s"Updating $dailyJobName Process so that it can pick the Start Timestamp ($startTimestamp) from here")
      val dailyEmailJobId = jc.getJobId(dailyJobName, targetTable, dailyJobName)
      val (dailyInstanceId, _) = jc.startJob(dailyEmailJobId)
      jc.endJob(dailyInstanceId, 1, startTimestamp, startTimestamp)

      jc.endJob(emailInstanceId, 1)
      jc.endJob(instanceId, 1)
    }
    catch {
      case e: Throwable  =>
        log.info(s"Something went WRONG during the EmailLookup run for Instance: $emailInstanceId")
        jc.endJob(emailInstanceId, -1)
        throw e
    }

  }

}

