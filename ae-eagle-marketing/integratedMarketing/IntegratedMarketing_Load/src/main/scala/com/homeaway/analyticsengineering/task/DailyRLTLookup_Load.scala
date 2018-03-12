package com.homeaway.analyticsengineering.task


import java.text.SimpleDateFormat
import com.amazonaws.services.s3.model.{ListObjectsV2Request, ListObjectsV2Result}
import com.homeaway.analyticsengineering.AnalyticsTaskApp._
import com.homeaway.analyticsengineering.encrypt.main.utilities.Utility
import org.apache.spark.sql.SaveMode
import org.joda.time.DateTime
import org.apache.spark.sql.functions._
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer


class DailyRLTLookup_Load extends Utility {


  def run(): Unit = {

    val jobId = jc.getJobId(conf("rlt.dailyJobName"), conf("rlt.objectName"), conf("rlt.dailyJobName"))
    val (rltInstanceId, rltLastSuccessfulRunDetails) = jc.startJob(jobId)

    val timeFormat = "yyyy-MM-dd HH:mm:ss"
    val timeFormatter = new SimpleDateFormat(timeFormat)
    val defaultStartTimestamp = DateTime.now.minusDays(1).toString(timeFormat)
    val startTimestamp= rltLastSuccessfulRunDetails.getOrElse("lastSuccessfulWaterMarkEndTime", defaultStartTimestamp)
    val endTimestamp = DateTime.now.toString(timeFormat)


    val s3aUri = conf("s3aUri")
    val clickstreamBucket = conf("clickstreamBucket")
    val sourcePrefix = conf("rlt.sourceDataLocationPrefix")
    val sourceDataFormat = conf("sourceDataFormat")
    val datePattern = """dateid=\d{8}""".r
    val targetDB = conf("aeStageDB")
    val targetTable = conf("rlt.objectName")
    val targetDataFormat = conf("targetDataFormat")
    val targetDataTmpLocation = conf("hdfsLoc") + "/" + conf("targetDataLocationPrefix") + "/" + conf("rlt.objectName") + conf("tmpString")
    val targetDataLocation = conf("s3aUri") + conf("s3StageBucket") + "/" + conf("targetDataLocationPrefix") + "/" + conf("rlt.objectName")

    val sql = spark.sql _

    spark.sparkContext.hadoopConfiguration.set("avro.mapred.ignore.inputs.without.extension", "false")

    try {

      jc.logInfo(rltInstanceId, s"START DailyEmailRLTDailyFact_Load Process", -1)

      log.info(s"Reading all Modified Directories in source data location from the last timestamp $startTimestamp")

      val ftp = new ArrayBuffer[String]()
      val req = new ListObjectsV2Request().withBucketName(clickstreamBucket).withPrefix(sourcePrefix+"/")
      var result = new ListObjectsV2Result
      do {
        result = s3.listObjectsV2(req)
        ftp ++= result.getObjectSummaries.filter { x => val modifiedTime = timeFormatter.format(x.getLastModified.getTime); modifiedTime > startTimestamp && modifiedTime <= endTimestamp && !Set("success", "folder").exists(e => x.getKey.toLowerCase.matches(s".*$e.*")) }.map(x => s3aUri + clickstreamBucket + "/" + x.getKey)
        req.setContinuationToken(result.getNextContinuationToken)
      } while (result.isTruncated)

      ftp.foreach(a=>println(a))

      val filesByDate :Map[String, Seq[String]]= ftp.groupBy(fs => datePattern.findFirstIn(fs).getOrElse("None").split("=").last).mapValues(_.toSeq)
      val datesToProcess = ftp.map(datePattern.findFirstIn(_).getOrElse(None)).distinct.filterNot(_ == None)
      val filesToProcess = ftp.toArray

      log.info(s"The following dates ${datesToProcess.mkString(",")} got modified from the last run at $startTimestamp")
      if (datesToProcess.isEmpty || filesToProcess.isEmpty) {
        log.info("No Dates to Process. Either the threshold is too high or the Upstream didn't write any files")
        log.info(s"Updating Job Instance $rltInstanceId with Status: SUCCEEDED, StartTime: $startTimestamp, EndTime: $endTimestamp")
        jc.logInfo(rltInstanceId, s"Updating Job Instance $rltInstanceId with Status: SUCCEEDED, StartTime: $startTimestamp, EndTime: $endTimestamp", -1)
        jc.endJob(rltInstanceId, 1, startTimestamp, endTimestamp)
        jc.endJob(instanceId, 1)
        cleanup()
        System.exit(0)
      }


      log.info(s"Reading visit_rlt_attribution_ga")
      val rlt_tmp= filesByDate.map{ files => spark.read.format(sourceDataFormat).load(files._2: _*).withColumn("dateid",lit(files._1))}.reduce(_ union _)
      rlt_tmp.createOrReplaceTempView("rlt_tmp")
      rlt_tmp.persist
      rlt_tmp.count


      log.info(s"Prepare RLT visit metrics ")
      var query =
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
           |FROM  rlt_tmp
           |WHERE rlt_marketingchannel='Email' AND rlt_marketingsubchannel='Email Marketing'
           |GROUP BY 1, 2, 3
        """.stripMargin

      log.info(s"Running Query: $query")
      val rlt_stage = sql(query)
      rlt_stage.createOrReplaceTempView("rlt_stage")
      rlt_stage.persist
      rlt_stage.count


      log.info("Building Email RLT Daily Fact")
      query =
        s"""
           |SELECT
           |      COALESCE(b.dateid, a.dateid) AS dateid
           |     ,COALESCE(b.fullvisitorid,a.fullvisitorid) AS fullvisitorid
           |     ,COALESCE(b.visitid,a.visitid) AS visitid
           |     ,COALESCE(b.rlt_campaign,a.rlt_campaign) AS rlt_campaign
           |     ,COALESCE(b.totalVisits,a.totalVisits) AS totalVisits
           |     ,COALESCE(b.datedSearch,a.datedSearch) AS datedSearch
           |     ,COALESCE(b.qualifiedVisit,a.qualifiedVisit) AS qualifiedVisit
           |FROM $targetDB.$targetTable a
           |FULL OUTER JOIN rlt_stage b
           |ON a.dateid = b.dateid
           |AND a.fullvisitorid = b.fullvisitorid
           |AND a.visitid = b.visitid
        """.stripMargin

      log.info(s"Running Query: $query")
      val rlt = sql(query)
      rlt.persist

      log.info(s"Saving Data to TEMP Target Location: $targetDataTmpLocation")
      saveDataFrameToDisk(rlt, SaveMode.Overwrite, targetDataFormat, targetDataTmpLocation)


      log.info(s"Read Data from TEMP Location: $targetDataTmpLocation")
      val final_metrics = spark.read.format(targetDataFormat).load(targetDataTmpLocation).coalesce(conf("rlt.numPartitions").toInt)

      log.info(s"Write Data to S3 Target Location: $targetDataLocation")
      saveDataFrameToDisk(final_metrics, SaveMode.Overwrite, targetDataFormat, targetDataLocation)

      log.info(s"Create Hive Table: $targetDB.$targetTable")
      checkAndCreateHiveDDL(hiveMetaStore, targetDB, targetTable, targetDataFormat, targetDataLocation, getColsFromDF(final_metrics))


      jc.logInfo(rltInstanceId, "END EmailRLTDailyFact_Load process", -1)


      jc.endJob(rltInstanceId, 1, startTimestamp, endTimestamp)
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
