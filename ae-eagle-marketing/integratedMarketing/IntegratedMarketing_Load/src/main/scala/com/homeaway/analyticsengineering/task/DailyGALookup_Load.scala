package com.homeaway.analyticsengineering.task

import java.text.SimpleDateFormat

import com.amazonaws.services.s3.model.{ListObjectsV2Request, ListObjectsV2Result}
import com.homeaway.analyticsengineering.AnalyticsTaskApp._
import com.homeaway.analyticsengineering.encrypt.main.utilities.Utility
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.lit
import org.joda.time.DateTime

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer


class DailyGALookup_Load extends Utility {

	def run(): Unit = {

		val jobId = jc.getJobId(conf("ga.dailyJobName"), conf("ga.objectName"), conf("ga.dailyJobName"))
		val (gaInstanceId, gaLastSuccessfulRunDetails) = jc.startJob(jobId)

		val timeFormat = "yyyy-MM-dd HH:mm:ss"
		val timeFormatter = new SimpleDateFormat(timeFormat)
		val defaultStartTimestamp = DateTime.now.minusDays(1).toString(timeFormat)
		val startTimestamp = gaLastSuccessfulRunDetails.getOrElse("lastSuccessfulWaterMarkEndTime", defaultStartTimestamp)
		val endTimestamp = DateTime.now.toString(timeFormat)


		val s3aUri = conf("s3aUri")
		val clickstreamBucket = conf("clickstreamBucket")
		val sourcePrefix = conf("ga.sourceDataLocationPrefix")
		val sourceDataFormat = conf("sourceDataFormat")
		val datePattern = """dateid=\d{8}""".r
		val targetDB = conf("aeStageDB")
		val targetTable = conf("ga.objectName")
		val targetDataFormat = conf("targetDataFormat")
		val targetDataTmpLocation = conf("hdfsLoc") + "/" + conf("targetDataLocationPrefix") + "/" + conf("ga.objectName") + conf("tmpString")
		val targetDataLocation = conf("s3aUri") + conf("s3StageBucket") + "/" + conf("targetDataLocationPrefix") + "/" + conf("ga.objectName")


		val sql = spark.sql _

		spark.sparkContext.hadoopConfiguration.set("avro.mapred.ignore.inputs.without.extension", "false")

		try {

			jc.logInfo(gaInstanceId, s"START DailyEmailGADailyFact_Load Process", -1)

			log.info(s"Reading all Modified Directories in source data location from the last timestamp $startTimestamp")

			val ftp = new ArrayBuffer[String]()
			val req = new ListObjectsV2Request().withBucketName(clickstreamBucket).withPrefix(sourcePrefix + "/")
			var result = new ListObjectsV2Result
			do {
				result = s3.listObjectsV2(req)
				ftp ++= result.getObjectSummaries.filter { x => val modifiedTime = timeFormatter.format(x.getLastModified.getTime); modifiedTime > startTimestamp && modifiedTime <= endTimestamp && !Set("success", "folder").exists(e => x.getKey.toLowerCase.matches(s".*$e.*")) }.map(x => s3aUri + clickstreamBucket + "/" + x.getKey)
				req.setContinuationToken(result.getNextContinuationToken)
			} while (result.isTruncated)

			ftp.foreach { a => println(a) }

			val filesByDate: Map[String, Seq[String]] = ftp.groupBy(fs => datePattern.findFirstIn(fs).getOrElse("None").split("=").last).mapValues(_.toSeq)
			val datesToProcess = ftp.map(datePattern.findFirstIn(_).getOrElse(None)).distinct.filterNot(_ == None)
			val filesToProcess = ftp.toArray

			log.info(s"The following dates ${datesToProcess.mkString(",")} got modified from the last run at $startTimestamp")
			if (datesToProcess.isEmpty || filesToProcess.isEmpty) {
				log.info("No Dates to Process. Either the threshold is too high or the Upstream didn't write any files")
				log.info(s"Updating Job Instance $gaInstanceId with Status: SUCCEEDED, StartTime: $startTimestamp, EndTime: $endTimestamp")
				jc.logInfo(gaInstanceId, s"Updating Job Instance $gaInstanceId with Status: SUCCEEDED, StartTime: $startTimestamp, EndTime: $endTimestamp", -1)
				jc.endJob(gaInstanceId, 1, startTimestamp, endTimestamp)
				jc.endJob(instanceId, 1)
				cleanup()
				System.exit(0)
			}


			log.info(s"Read Enriched_Events_GA ")

			val enriched_events_ga_tmp = filesByDate.map { files => spark.read.format(sourceDataFormat).load(files._2: _*).withColumn("dateid", lit(files._1)).select("dateid", "utm_medium", "utm_source", "utm_campaign", "fullVisitorId", "visitId", "page_url_path", "page_url") }.reduce(_ union _).where("utm_medium = 'email' AND lower(utm_source) IN ('cyc','cycle', 'adhoc','adh')")
			enriched_events_ga_tmp.createOrReplaceTempView("enriched_events_ga_tmp")
			enriched_events_ga_tmp.persist
			enriched_events_ga_tmp.count


			log.info(s"Get haexternalsourceid's from Clickstream EnrichedEventsGA")
			var query =
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
				  |FROM enriched_events_ga_tmp
				  |)
				  |GROUP BY 1,2
				""".stripMargin

			log.info(s"Running Query: $query")
			val enriched_events_ga_stage = sql(query)
			enriched_events_ga_stage.persist
			enriched_events_ga_stage.count
			enriched_events_ga_stage.createOrReplaceTempView("enriched_events_ga_stage")


			query =
				s"""
				   |SELECT
				   |        COALESCE(b.fullvisitorid,a.fullvisitorid) AS fullvisitorid
				   |       ,COALESCE(b.visitid,a.visitid) AS visitid
				   |       ,COALESCE(b.dateId,a.dateId) as dateid
				   |       ,COALESCE(b.ga_campaign, a.ga_campaign) as ga_campaign
				   |       ,COALESCE(b.haexternalsourceid, a.haexternalsourceid) as haexternalsourceid
				   |FROM $targetDB.$targetTable a
				   |FULL OUTER JOIN enriched_events_ga_stage b
				   |ON a.fullvisitorid = b.fullvisitorid
				   |AND a.visitid = b.visitid
          """.stripMargin

			log.info(s"Running Query: $query")
			val enriched_events_ga = sql(query)
			enriched_events_ga.persist
			enriched_events_ga.count
			enriched_events_ga.createOrReplaceTempView("enriched_events_ga")


			log.info(s"Saving Data to TEMP Target Location: $targetDataTmpLocation")
			saveDataFrameToDisk(enriched_events_ga, SaveMode.Overwrite, targetDataFormat, targetDataTmpLocation)


			log.info(s"Read Data from TEMP Location: $targetDataTmpLocation")
			val final_metrics = spark.read.format(targetDataFormat).load(targetDataTmpLocation).coalesce(conf("ga.numPartitions").toInt)

			log.info(s"Write Data to S3 Target Location: $targetDataLocation")
			saveDataFrameToDisk(final_metrics, SaveMode.Overwrite, targetDataFormat, targetDataLocation)

			log.info(s"Create Hive Table: $targetDB.$targetTable")
			checkAndCreateHiveDDL(hiveMetaStore, targetDB, targetTable, targetDataFormat, targetDataLocation, getColsFromDF(final_metrics))


			jc.logInfo(gaInstanceId, "END EmailGADailyFact_Load process", -1)


			jc.endJob(gaInstanceId, 1, startTimestamp, endTimestamp)
			jc.endJob(instanceId, 1)
		}
		catch {
			case e: Throwable =>
				log.info(s"Something went WRONG during the GA run for Instance: $gaInstanceId")
				jc.endJob(gaInstanceId, -1)
				throw e
		}


	}

}

