package com.homeaway.analyticsengineering.task

/**
  * Created by aguyyala on 10/19/17.
  */


import com.homeaway.analyticsengineering.AnalyticsTaskApp._
import com.homeaway.analyticsengineering.encrypt.main.utilities.Utility
import com.homeaway.analyticsengineering.encrypt.main.loader.EDWLoader
//import org.apache.spark.sql.SaveMode
//import java.text.SimpleDateFormat


class $name$ extends Utility {


	def run(): Unit = {

		val edwLoader = new EDWLoader(spark)

		// 	val sourceDB = spark.conf.get("targetDB")
		// 	val sourceTable = spark.conf.get("sourceTable")
		// 	val sourceUnknownDataLocationPrefix = spark.conf.get("sourceDataLocationPrefix")
		// 	val sourceUnknownDataLocation = spark.conf.get("s3aUri") + spark.conf.get("s3Bucket") + "/" + sourceUnknownDataLocationPrefix

		// 	val targetDB = spark.conf.get("targetDB")
		// 	val targetTable = spark.conf.get("targetTable")
		// 	val targetDataFormat = spark.conf.get("tagetDataFormat")
		// 	val targetDataLocation = spark.conf.get("s3aUri") + spark.conf.get("s3Bucket") + "/" + spark.conf.get("targetDataLocationPrefix")
		// 	val tmpString = spark.conf.get("tmpString")
		// 	val targetDataTmpLocation = spark.conf.get("hdfsLoc") + "/" + spark.conf.get("targetDataLocationPrefix") + tmpString
		// 	val targetPartCols = Array("send_date")

		// 	val offset = spark.conf.get("offset").toInt

		// 	val sql = spark.sql _

		// 	val dateFormat = "yyyyMMdd"
		// 	val dateFormatter = DateTimeFormat.forPattern(dateFormat)

		// 	val timeFormat = "yyyy-MM-dd HH:mm:ss"
		// 	val timeFormatter = new SimpleDateFormat(timeFormat)

		// 	val endDate = conf.getOrElse("endDate", (DateTime.now - 1.day).toString(dateFormat))
		// 	val startDate = conf.getOrElse("startDate", DateTime.parse(endDate, dateFormatter).minusDays(offset).toString(dateFormat))

		// 	val defaultStartTimestamp = (DateTime.now - 1.day).toString(timeFormat)
		// 	val startTimestamp = lastSuccessfulRunDetails.getOrElse("lastSuccessfulWaterMarkEndTime", defaultStartTimestamp)
		// 	val endTimestamp = DateTime.now.toString(timeFormat)


		try {

			jc.logInfo(instanceId, s"START $name $ Process", -1)

			// YOUR CODE HERE


			/*  Sample Code 1:

			val sqlHiveTable = s"SELECT * FROM \$targetDB.\$targetTable"

			log.info(s"Running Query: \$sqlHiveTable")
			val dfSqlHiveTable = spark.read.format(finalFormat).load(getHiveTableLocation(hiveMetaStore, targetDB, targetTable))

			dfSqlHiveTable.persist

			dfSqlHiveTable.createOrReplaceTempView("tmpSqlHiveTable")


			val sqlServerDummy = s"SELECT * FROM dw.dbo.dummy"

			log.info(s"Running Query: \$sqlServerDummy")
			val dfSqlServerDummy = edwLoader.getEDWData(sqlServerDummy, 40)

			dfSqlServerDummy.persist

			dfSqlServerDummy.createOrReplaceTempView("tmpSqlServerDummy")


			var query =
				"""
				  |SELECT
				  | *
				  |FROM hiveDummyTable1
				""".stripMargin

			log.info(s"Running Query: \$query")
			val dfStage = sql(query).repartition(50)

			dfStage.persist

			debug(dfStage, "dfStage")

			// The below query is just to show that you don't have to define `var query` again,
			// you can just simply use `query = ...`
			query =
				"""
				  |SELECT
				  | *
				  |FROM hiveDummyTable2
				""".stripMargin

			log.info(s"Running Query: \$query")
			val dfFinal = sql(query).repartition(50)

			val fCount = dfFinal.count

			val fmsg = "$name$ - Final Count:"
			log.info(s"\$fmsg \$fCount")
			jc.logInfo(instanceId, fmsg, fCount)


			log.info(s"Saving Data to Temp Location: \$tmpLocation")
			saveDataFrameToDisk(dfFinal, SaveMode.Overwrite, finalFormat, tmpLocation)

			log.info("Moving Data from Temp to Final Location")
			hdfsRemoveAndMove(dfs, dfc, tmpLocation, finalLocation)

			log.info("Check and Create Hive DDL")
			checkAndCreateHiveDDL(hiveMetaStore, targetDB, targetTable, finalFormat, finalLocation, getColsFromDF(dfFinal))

			log.info("Purge Old Snapshots of $objectName$")
			purgeHDFSPath(dfs, dfc, finalBaseLoc, retentionDays)

			log.info("Altering Table's Location to new Location")
			alterTableLocation(hiveMetaStore, targetDB, targetTable, dfs.getUri.toString + finalLocation)

			jc.logInfo(instanceId, "END $name$ Process", -1)
			*/


			/* Sample Code 2:

			log.info(s"Reading all Modified Files in \$sourceUnknownDataLocation from the last timestamp \$startTimestamp")

			val req = new ListObjectsV2Request().withBucketName(conf("s3Bucket")).withPrefix(sourceUnknownDataLocationPrefix)
			var result = new ListObjectsV2Result
			val ftp = new ArrayBuffer[String]()
			val dtPattern = (targetPartCols.head + """=\d{8}""").r


			do {
				result = s3.listObjectsV2(req)
				ftp ++= result.getObjectSummaries.filter { x => val modifiedTime = timeFormatter.format(x.getLastModified.getTime); modifiedTime > startTimestamp && modifiedTime <= endTimestamp && !Set("success", "folder").exists(e => x.getKey.toLowerCase.matches(s".*\$e.*")) }.map(x => spark.conf.get("s3aUri") + spark.conf.get("s3Bucket") + "/" + x.getKey)
				req.setContinuationToken(result.getNextContinuationToken)
			} while (result.isTruncated)

			val datesToProcess = ftp.map(dtPattern.findFirstIn(_).getOrElse(None)).distinct.filterNot(_ == None)
			val filesToProcess = ftp.toArray

			log.info(s"The following dates \${datesToProcess.mkString(",")} got modified for Unknown Data from the last run at \$startTimestamp")
			if (datesToProcess.isEmpty || filesToProcess.isEmpty) {
				log.info("No Dates to Process for Unknown data. Either the threshold is too high or the Upstream didn't write any files")
				log.info("Creating maeUnknown Empty Dataframe")
				val maeUnknown = sql(s"SELECT * FROM \$sourceDB.\$sourceTable WHERE 1 = 2")
				maeUnknown.createOrReplaceTempView("maeUnknown")
			}
			else {
				log.info("Reading unknown sourceemailrecipientids from source")
				val maeUnknown = spark.read.format(conf("sourceDataFormat")).load(filesToProcess: _*).distinct
				maeUnknown.createOrReplaceTempView("maeUnknown")
			}

			log.info(s"""Reading and Aggregating all Unknown Data for date \${conf("defaultDate")}""")
			var query =
				s"""
				   |SELECT
				   |     ...
				""".stripMargin

			log.info(s"Running Query: \$query")
			val marketingActivityCampaignSendFactUnknown = sql(query).coalesce(1)
			marketingActivityCampaignSendFactUnknown.createOrReplaceTempView("marketingActivityCampaignSendFactUnknown")


			log.info(s"Reading and Aggregating Source Data by SendDate, AssetName and CampaignId for the last 90 days between \$startDate AND \$endDate")
			query =
				s"""
				   |SELECT
				   |      ...
				""".stripMargin

			log.info(s"Running Query: \$query")
			val marketingActivityCampaignSendFact = sql(query).distinct.coalesce(1)
			marketingActivityCampaignSendFact.persist
			marketingActivityCampaignSendFact.createOrReplaceTempView("marketingActivityCampaignSendFact")

			log.info(s"Saving Data to TEMP target Location: \$targetDataTmpLocation")
			saveDataFrameToDisk(marketingActivityCampaignSendFact, SaveMode.Overwrite, targetDataFormat, targetDataTmpLocation)

			log.info(s"Saving Data to Final target Location: \$targetDataLocation")
			saveDataFrameToDisk(spark.read.format(targetDataFormat).load(targetDataTmpLocation).coalesce(1), SaveMode.Overwrite, targetDataFormat, targetDataLocation)

			log.info("Check and Create Hive DDL")
			checkAndCreateHiveDDL(hiveMetaStore, targetDB, targetTable, targetDataFormat, targetDataLocation, getColsFromDF(marketingActivityCampaignSendFact))

			log.info("Attributing Unknown Data")
			query =
				s"""
				   |SELECT
				   |      ...
				""".stripMargin


			log.info(s"Running Query: \$query")
			val marketingActivityCampaignSendFactStage = sql(query).coalesce(1)
			marketingActivityCampaignSendFactStage.createOrReplaceTempView("marketingActivityCampaignSendFactStage")

			query =
				s"""
				   |SELECT
				   |      ...
				""".stripMargin

			log.info(s"Running Query: \$query")
			val marketingActivityCampaignSendFactFinal = sql(query).distinct.coalesce(1)

			log.info(s"Saving Data to target Location: \$finalDataLocation")
			saveDataFrameToDisk(marketingActivityCampaignSendFactFinal, SaveMode.Overwrite, targetDataFormat, finalDataLocation)

			log.info("Check and Create Hive DDL")
			checkAndCreateHiveDDL(hiveMetaStore, targetDB, finalTable, targetDataFormat, finalDataLocation, getColsFromDF(marketingActivityCampaignSendFactFinal))

			//			log.info(s"Saving Data to TEMP target Location: \$targetDataTmpLocation")
			//			saveDataFrameToDisk(marketingActivityCampaignSendFactFinal, SaveMode.Overwrite, targetDataFormat, targetDataTmpLocation)

			//			log.info(s"Saving Data to Final target Location: \$targetDataTmpLocation")
			//			saveDataFrameToDisk(spark.read.format(targetDataFormat).load(targetDataTmpLocation).coalesce(1), SaveMode.Overwrite, targetDataFormat, targetDataLocation)

			//			val tmpDirs = dfs.globStatus(new Path(targetDataTmpLocation + "/" + "*" * targetPartCols.length)).map(fs => fs.getPath.toString)
			//
			//			val keys = new ArrayBuffer[String]()
			//			for (i <- tmpDirs) {
			//				val Array(_, partition) = i.split(tmpString)
			//				val dir = conf("targetDataLocationPrefix") + partition
			//				log.info(s"Deleting Existing Data \$dir")
			//				val req = new ListObjectsV2Request().withBucketName(conf("s3Bucket")).withPrefix(dir)
			//				var result = new ListObjectsV2Result
			//
			//				do {
			//					result = s3.listObjectsV2(req)
			//					keys ++= result.getObjectSummaries.map(x => x.getKey)
			//					req.setContinuationToken(result.getNextContinuationToken)
			//				} while (result.isTruncated)
			//
			//			}
			//
			//			val multiObjectDeleteRequest = new DeleteObjectsRequest(conf("s3Bucket")).withKeys(keys: _*)
			//			s3.deleteObjects(multiObjectDeleteRequest)
			//
			//			val localDirPath = "/tmp" + targetDataTmpLocation
			//			val localDir = new File(localDirPath)
			//			delDir(localDir)
			//
			//			log.info(s"Copy Data from \$targetDataTmpLocation to \$localDirPath")
			//			dfs.copyToLocalFile(false, new Path(targetDataTmpLocation), new Path(localDirPath), true)
			//
			//			log.info(s"Upload Data to S3 from \$localDirPath to \$targetDataLocation")
			//			val uploadCampaignData = tx.uploadDirectory(conf("s3Bucket"), conf("targetDataLocationPrefix"), localDir, true)
			//			uploadCampaignData.waitForCompletion()
			//			delDir(localDir)

			//			log.info("Add Hive Partitions")
			//			addHivePartitions(hiveMetaStore, targetDB, targetTable, constructHivePartitions(marketingActivityCampaignSendFact, targetPartCols, targetDataLocation))
			//			marketingActivityCampaignSendFact.unpersist

			jc.logInfo(instanceId, "END $name$ process", -1)
			*/

			jc.endJob(instanceId, 1)
		}
		catch {
			case e: Throwable =>
				log.error(s"Something went WRONG during the run ")
				log.error(e.printStackTrace())
				jc.endJob(instanceId, -1)
				System.exit(1)
		}
		finally {
			
		}

		//		def debug(df: DataFrame, dfName: String): Unit = {
		//
		//			log.info(s"Debug Mode Enabled, writing temp data \$df to HDFS Location: \${debugLocation + "/" + dfName}")
		//			saveDataFrameToHdfs(df, SaveMode.Overwrite, finalFormat, debugLocation + "/" + dfName)
		//
		//		}


		//		def delDir(dir: File): Unit = {
		//
		//			if (dir.exists) {
		//				log.info(s"Directory/File Exists, Deleting \${dir.getAbsolutePath}")
		//				FileUtils.forceDelete(dir)
		//			}
		//		}

	}

}
