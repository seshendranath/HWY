package com.homeaway.analyticsengineering.encrypt.main.utilities

/**
  * Created by aguyyala on 6/21/17.
  */


import java.util.{Properties, UUID}

import com.datastax.driver.core.utils.UUIDs
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.Options.Rename
import org.apache.hadoop.hive.metastore.api._
import org.apache.hadoop.hive.metastore.TableType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.log4j.{Level, Logger}
import org.joda.time.format.DateTimeFormat

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source
import org.springframework.boot.ApplicationArguments
import com.amazonaws.services.s3.model.DeleteObjectsRequest
import org.apache.hadoop.fs.{FileContext, FileSystem}
import com.amazonaws.services.s3.AmazonS3ClientBuilder

import scala.collection.JavaConversions._
import com.amazonaws.services.s3.model.{ListObjectsV2Request, ListObjectsV2Result}

import scala.collection.mutable.ArrayBuffer
import scala.sys.process._
import com.amazonaws.services.s3.AmazonS3URI

import scala.util.Try
import org.apache.spark.sql._
import com.github.nscala_time.time.Imports._
import org.apache.hadoop.hive.common.FileUtils

trait Utility {

	val log: Logger = Logger.getLogger(getClass)
	log.setLevel(Level.toLevel("Info"))


	val formatInfo = Map(
		"orc" -> Map("serde" -> "org.apache.hadoop.hive.ql.io.orc.OrcSerde",
			"inputFormat" -> "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
			"outputFormat" -> "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat"),
		"parquet" -> Map("serde" -> "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
			"inputFormat" -> "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
			"outputFormat" -> "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"),
		"csv" -> Map("serde" -> "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
			"inputFormat" -> "org.apache.hadoop.mapred.TextInputFormat",
			"outputFormat" -> "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")
	)


	/**
	  * This module will handle Skewness and Small Files Problem.
	  * While writing the dataframe to hdfs, it will create the number of files for each partition based on the passed map
	  *
	  * @param df                     : DataFrame to be Repartitioned
	  * @param mainPartCol            : Main Partitioning Column to define number of files
	  * @param partCols               : All the partitioning Columns
	  * @param outNoFilesPerPartition : Map of outNoFiles Per Main Partition
	  * @param defaultOutputNoFiles   : Default Number of Output Files if the value is not found in passed Map
	  * @return Repartitioned DataFrame along with Fake column to generate desired no of files
	  *
	  *
	  *         Eg:
	  *
	  *         Call:
	  *         dataFrameOutputFiles(emailDF, country_code, Array("sendDate", "country_code"), Map("US" -> 160, "UK" -> 40), 10)
	  *
	  *         Return's Repartitioned DataFrame that will Result in:
	  *         Country US -> Spark will produce 160 (specified in Map) files
	  *         Country UK -> Spark will produce 40 (specified in Map) files
	  *         Rest of the Countries -> Spark will produce 10 (defaultOutputNoFiles) files
	  *
	  */
	def dataFrameOutputFiles(df: DataFrame, mainPartCol: String, partCols: Seq[String], outNoFilesPerPartition: Map[String, Int], defaultOutputNoFiles: Int): DataFrame = {
		val fCol: Column = expr(outNoFilesPerPartition.map { case (part, numFiles) =>
			s" WHEN $mainPartCol = '$part' THEN floor(pmod(rand() * 100, $numFiles))"
		}.mkString("CASE\n", "\n", s"\nELSE floor(pmod(rand() * 100, $defaultOutputNoFiles)) END"))

		df.repartition(partCols.map(c => col(c)) :+ fCol: _*)
	}


	def logSparkConf(spark: SparkSession): Unit = log.info(s"Logging spark confs: ${spark.conf.getAll.filterNot(x => Set("password", "key", "salt", "token", "credential", "java").exists(e => x._1.toLowerCase.matches(s".*$e.*"))).toString}")


	def setConf(args: ApplicationArguments, spark: SparkSession): Unit = args.getOptionNames.asScala.foreach { c => spark.conf.set(c, args.getOptionValues(c).asScala.headOption.getOrElse("true")) }


	def getProps(fName: String): Map[String, String] = {
		Source.fromFile(fName).getLines.map(_.split("=")).map { case Array(k, v) => (k, v) }.toMap
	}


	def setPropertiesFromGit(spark: SparkSession): Unit = {
		val git = new GitUtils()

		val repo = git.cloneRepo(spark.conf.get("spark.git.gitURI"), spark.conf.get("spark.git.authToken"))
		val lastCommitId = git.getLastCommit(repo)

		spark.conf.get("spark.git.credentialsFileName").split(",").foreach { file =>
			val contents = git.getContentsofFile(repo, lastCommitId, file)

			var props = mutable.Map[String, String]()

			contents.split('\n').foreach { x =>
				val Array(k, v) = x.split("=", 2)
				props += (k -> v)
				spark.conf.set(k, v)
			}

		}

		git.cleanup(repo)

	}


	/**
	  * Extracts Timestamp from TimeUUID
	  *
	  * @param tuid : TimeUUID
	  * @return Timestamp (format: YYYYMMddHHmmss)
	  */
	def extractTimeFromTimeUUID(tuid: String): String = {
		val NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L
		val epochMillis = (UUID.fromString(tuid).timestamp() - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000
		DateTimeFormat.forPattern("YYYYMMddHHmmss").print(epochMillis)
	}


	/**
	  * Generates Time Based UUID
	  *
	  * @return TimeUUID
	  */
	def generateTimeUUID: String = UUIDs.timeBased().toString


	def sqlservercredentials(args: ApplicationArguments, sparkSession: SparkSession): Unit = {
		if (args.getOptionValues("user") != null & args.getOptionValues("password") != null) {
			sparkSession.conf.set("user", args.getOptionValues("user").get(0))
			sparkSession.conf.set("password", args.getOptionValues("password").get(0))
		}
	}


	def ETLControlLogLoader(jc: JobControl, spark: SparkSession, snapshotDate: String, objectName: String, ProcessActionName: String, TempView: String): Unit = {
		val dfCount = spark.sql(s"SELECT CAST(COUNT(*) AS Long) AS count FROM $TempView").select("count").collect()(0).getLong(0)
		jc.logInfo(spark.conf.get("instanceId"), ProcessActionName, dfCount)
	}


	def isRun(args: ApplicationArguments): Boolean = {
		if (args.containsOption("className")) args.getOptionValues("className").asScala.toList.exists(value => value.equals(this.getClass.getSimpleName))
		else true
	}


	def EDWWriter(spark: SparkSession, ds: DataFrame, tb: String, db: String): Unit = {

		val props = new Properties()
		props.put("user", spark.conf.get("user"))
		props.put("password", spark.conf.get("password"))
		props.put("driver", spark.conf.get("driver"))

		val url = spark.conf.get("url") + ";database =" + db

		ds.write.mode(SaveMode.Append).jdbc(url, tb, props)

	}

	/**
	  * Returns Hive equivalent DataType for Spark's DataType
	  *
	  * @param dataType : Spark's DataType
	  * @return Hive's DataType
	  */
	def sparktoHiveDataType(dataType: String): String = {
		val decimalPattern = """DecimalType\(\d+,\s?\d+\)""".r

		dataType match {
			case "StringType" => "string"
			case "LongType" => "bigint"
			case "DoubleType" => "double"
			case "IntegerType" => "int"
			case "DateType" => "date"
			case "TimestampType" => "timestamp"
			case "BooleanType" => "boolean"
			case decimalPattern() => dataType.replace("Type", "").toLowerCase
			case _ => "string"
		}
	}

	/**
	  * Mapping Input/Output formats from Hive to Spark
	  */
	def getSparkFileFormat(fileFormat: String): String = {
		val format = fileFormat.toLowerCase

		format match {
			case _ if format contains "parquet" => "parquet"
			case _ if format contains "orc" => "orc"
		}

	}


	/**
	  * Get Hive Table's Input Format
	  *
	  * @param hiveMetaStore : HiveMetaStore
	  * @param dbName        : Database name
	  * @param tableName     : Table name
	  * @return Hive Table's Input Format
	  */
	def getHiveInputFormat(hiveMetaStore: HiveMetaStoreClient, dbName: String, tableName: String): String = {
		val hiveFormat = hiveMetaStore.getTable(dbName, tableName).getSd.getInputFormat
		getSparkFileFormat(hiveFormat)
	}


	/**
	  * Save DataFrame to HDFS
	  *
	  * @param df       : DataFrame to be saved
	  * @param path     : HDFS Path to be saved to
	  * @param format   : Input Format
	  * @param saveMode : SaveMode Overwrite OR Append
	  */
	def saveDataFrameToHdfs(df: DataFrame, saveMode: SaveMode, format: String, path: String, partCols: Array[String] = Array()): Unit = {
		log.info(s"Writing DataFrame to $path")
		if (format != "csv") {
			if (partCols.nonEmpty) df.write.mode(saveMode).format(format).partitionBy(partCols: _*).save(path)
			else df.write.mode(saveMode).format(format).save(path)
		}
		else {
			if (partCols.nonEmpty) df.write.mode(saveMode).format(format).option("sep", "\u0001").partitionBy(partCols: _*).save(path)
			else df.write.mode(saveMode).format(format).option("sep", "\u0001").save(path)
		}

	}


	/**
	  * Save DataFrame to Disk - HDFS/S3
	  *
	  * @param df       : DataFrame to be saved
	  * @param path     : HDFS/S3 Path to be saved to
	  * @param format   : Input Format
	  * @param saveMode : SaveMode Overwrite OR Append
	  */
	def saveDataFrameToDisk(df: DataFrame, saveMode: SaveMode, format: String, path: String, partCols: Array[String] = Array()): Unit = {
		log.info(s"Writing DataFrame to $path")
		if (format != "csv") {
			if (partCols.nonEmpty) df.write.mode(saveMode).format(format).partitionBy(partCols: _*).save(path)
			else df.write.mode(saveMode).format(format).save(path)
		}
		else {
			if (partCols.nonEmpty) df.write.mode(saveMode).format(format).option("sep", "\u0001").partitionBy(partCols: _*).save(path)
			else df.write.mode(saveMode).format(format).option("sep", "\u0001").save(path)
		}

	}


	/**
	  * Extract Column and DataType(Hive's) Information from DataFrame
	  *
	  * @param df : DataFrame whose column and datatype info to be extracted
	  * @return Array((column, datatype(Hive)))
	  */
	def getColsFromDF(df: DataFrame, exclude: Seq[String] = Seq()): Array[(String, String)] = {
		log.info("Extracting Column Info from DataFrame")
		val cols = mutable.ArrayBuffer[(String, String)]()
		for (column <- df.dtypes) {
			val (col, dataType) = column
			if (!(exclude contains col)) {
				cols += ((col, sparktoHiveDataType(dataType)))
			}
		}
		cols.toArray
	}

	/**
	  * Extract Column and DataType(Hive's) Information from DataFrame
	  *
	  * @param df : DataFrame whose column and datatype info to be extracted
	  * @return Array(column)
	  */
	def getOnlyColsFromDF(df: DataFrame, exclude: Seq[String] = Seq()): Array[(String)] = {
		log.info("Extracting Column Info from DataFrame")
		val cols = mutable.ArrayBuffer[(String)]()
		for (column <- df.dtypes) {
			val (col, _) = column
			if (!(exclude contains col)) {
				cols += col
			}
		}
		cols.toArray
	}

	/**
	  * Extract Partition Columns and DataType(Hive's) Information from DataFrame
	  *
	  * @param df : DataFrame whose column and datatype info to be extracted
	  * @return Array((column, datatype(Hive)))
	  */
	def getPartColsFromDF(df: DataFrame, exclude: Seq[String] = Seq()): Array[(String, String)] = {
		log.info("Extracting Column Info from DataFrame")
		val cols = mutable.ArrayBuffer[(String, String)]()
		for (column <- df.dtypes) {
			val (col, dataType) = column
			if (exclude contains col) {
				cols += ((col, sparktoHiveDataType(dataType)))
			}
		}
		cols.toArray
	}


	/**
	  * Get HDFS Location of Hive Table
	  *
	  * @param hiveMetaStore : HiveMetaStore
	  * @param dbName        : Database name
	  * @param tableName     : Table name
	  * @return HDFS Location of Hive Table
	  */
	def getHiveTableLocation(hiveMetaStore: HiveMetaStoreClient, dbName: String, tableName: String): String = {
		hiveMetaStore.getTable(dbName, tableName).getSd.getLocation
	}


	/**
	  * Alter Location of the Hive Table
	  *
	  * @param hiveMetaStore : HiveMetaStore
	  * @param db            : Database name
	  * @param tbl           : Table name
	  * @param loc           : New HDFS Location to be applied
	  */
	def alterTableLocation(hiveMetaStore: HiveMetaStoreClient, db: String, tbl: String, loc: String): Unit = {
		val newTbl = hiveMetaStore.getTable(db, tbl)
		val newSd = newTbl.getSd
		newSd.setLocation(loc)
		newTbl.setSd(newSd)
		hiveMetaStore.alter_table(db, tbl, newTbl)
	}


	/**
	  * Checks whether passed DDL is identical to Hive Table's DDL
	  * Added partition columns also derived from df schema with datatype
	  *
	  * @param hiveMetaStore : HiveMetaStore
	  * @param dbName        : Database name
	  * @param tblName       : Table name
	  * @param cols          : Column and Datatype Info, checks the Column Names, DataType Info, and Ordering of columns as well
	  * @return True if matches else False
	  */
	def checkDDL(hiveMetaStore: HiveMetaStoreClient, dbName: String, tblName: String, cols: Seq[(String, String)], partCols: Seq[(String, String)] = Seq()): Boolean = {

		val tbl = hiveMetaStore.getTable(dbName, tblName)
		val hiveCols = tbl.getSd.getCols.asScala.map(x => (x.getName, x.getType)).toArray
		val hivePartCols = tbl.getPartitionKeys.asScala.map(x => (x.getName, x.getType)).toArray
		(cols.map(x => (x._1.toLowerCase, x._2.toLowerCase)) ++ partCols.map(x => (x._1.toLowerCase, x._2.toLowerCase))).toArray.deep == (hiveCols ++ hivePartCols).deep

	}

	/**
	  * Once table is modified and rebuild exisitng partitions - alternative to msck repair table or recover partitions (EMR)
	  *
	  * @param hiveMetaStore : HiveMetaStore
	  * @param dbName        : Database name
	  * @param tblName       : Table name
	  * @param parts         : Partition List
	  */

	def recoverpartitions(hiveMetaStore: HiveMetaStoreClient, dbName: String, tblName: String, parts: List[String]): Unit = {

		val table = hiveMetaStore.getTable(dbName, tblName)
		val loc = table.getSd.getLocation

		val partitions = for (part <- parts) yield {
			val p: List[String] = List(part.split("=")(1))
			val partloc = loc + "/" + part
			val partition = new Partition()
			partition.setDbName(dbName)
			partition.setTableName(tblName)
			val sd = new StorageDescriptor(table.getSd)
			sd.setLocation(partloc)
			partition.setSd(sd)
			partition.setValues(p.asJava)
			partition
		}
		hiveMetaStore.add_partitions(partitions.asJava, true, true)
	}

	/**
	  * 1. Check's Table existence and create a new Table if it doesn't exist
	  * 2. If Table exists, checks the DDL info and create the new Table if the existing Hive DDL is not mnatching with the passed DDL'
	  *
	  * @param hiveMetaStore : HiveMetaStore
	  * @param dbName        : Database name
	  * @param tblName       : Table name
	  * @param format        : Table's Input Format
	  * @param location      : HDFS Location of the Table
	  * @param cols          : Column and DataType info
	  */
	def checkAndCreateHiveDDL(hiveMetaStore: HiveMetaStoreClient, dbName: String, tblName: String, format: String,
	                          location: String, cols: Seq[(String, String)], partCols: Seq[(String, String)] = Seq()): Unit = {

		val tblExist = hiveMetaStore.tableExists(dbName, tblName)

		val ddlMatch = if (tblExist) checkDDL(hiveMetaStore, dbName, tblName, cols, partCols) else false

		if (!tblExist || !ddlMatch) {
			log.info("TABLE Does Not Exists OR DDL Mismatch, Creating New One")

			if (tblExist) hiveMetaStore.dropTable(dbName, tblName)

			val s = new SerDeInfo()
			s.setSerializationLib(formatInfo(format)("serde"))
			s.setParameters(Map("serialization.format" -> "1").asJava)

			val sd = new StorageDescriptor()
			sd.setSerdeInfo(s)
			sd.setInputFormat(formatInfo(format)("inputFormat"))
			sd.setOutputFormat(formatInfo(format)("outputFormat"))

			val tblCols = cols.map { case (c, dataType) => new FieldSchema(c, dataType, c) }.toList
			sd.setCols(tblCols.asJava)

			sd.setLocation(location)

			val t = new Table()

			t.setSd(sd)

			if (partCols.nonEmpty) {
				val tblPartCols = partCols.map { case (c, dataType) => new FieldSchema(c, dataType, c) }.toList
				t.setPartitionKeys(tblPartCols.asJava)
			}

			t.setTableType(TableType.EXTERNAL_TABLE.toString)
			t.setDbName(dbName)
			t.setTableName(tblName)
			t.setParameters(Map("EXTERNAL" -> "TRUE", "tableType" -> "EXTERNAL_TABLE").asJava)

			hiveMetaStore.createTable(t)

		}
	}

	/**
	  * 1. Check's Table existence and create a new Table if it doesn't exist
	  * 2. If Table exists, checks the DDL info and create the new Table if the existing Hive DDL is not matching with the passed DDL'
	  * 3. Added partition columns also derived from df schema with datatype
	  *
	  * @param hiveMetaStore     : HiveMetaStore
	  * @param dbName            : Database name
	  * @param tblName           : Table name
	  * @param format            : Table's Input Format
	  * @param location          : HDFS Location of the Table
	  * @param cols              : Column and DataType info
	  * @param recoverPartitions : Flag to recover paritions when process recreates partitinoed table
	  */

	def checkAndCreateHiveDDLRecoverParts(hiveMetaStore: HiveMetaStoreClient, dbName: String, tblName: String, format: String,
	                                      location: String, cols: Seq[(String, String)], partCols: Seq[(String, String)] = Seq(), recoverPartitions: Boolean = true): Unit = {

		val tblExist = hiveMetaStore.tableExists(dbName, tblName)

		val ddlMatch = if (tblExist) checkDDL(hiveMetaStore, dbName, tblName, cols, partCols) else false

		if (!tblExist || !ddlMatch) {
			log.info("TABLE Does Not Exists OR Schema Mismatch, Creating New One")

			val s = new SerDeInfo()
			s.setSerializationLib(formatInfo(format)("serde"))
			s.setParameters(Map("serialization.format" -> "1").asJava)

			val sd = new StorageDescriptor()
			sd.setSerdeInfo(s)
			sd.setInputFormat(formatInfo(format)("inputFormat"))
			sd.setOutputFormat(formatInfo(format)("outputFormat"))

			val tblCols = cols.map { case (c, dataType) => new FieldSchema(c, dataType, c) }.toList
			sd.setCols(tblCols.asJava)

			sd.setLocation(location)

			val t = new Table()

			t.setSd(sd)

			if (partCols.nonEmpty) {
				val tblPartCols = partCols.map { case (c, dataType) => new FieldSchema(c, dataType, c) }.toList
				t.setPartitionKeys(tblPartCols.asJava)
			}

			t.setTableType(TableType.EXTERNAL_TABLE.toString)
			t.setDbName(dbName)
			t.setTableName(tblName)
			t.setParameters(Map("EXTERNAL" -> "TRUE", "tableType" -> "EXTERNAL_TABLE").asJava)

			log.info(s"print create statement $t")


			if (tblExist && partCols.nonEmpty) {

				val partitionlist = hiveMetaStore.listPartitionNames(dbName, tblName, 32767).asScala.toList

				if (tblExist) hiveMetaStore.dropTable(dbName, tblName)

				hiveMetaStore.createTable(t)

				log.info(s"TABLE has creted with new Schema, recovering partitions now for $dbName $tblName")

				if (recoverPartitions) {
					recoverpartitions(hiveMetaStore, dbName, tblName, partitionlist)
				}

				log.info(s"recovered partitions for $dbName $tblName")
			}
			{
				if (tblExist) hiveMetaStore.dropTable(dbName, tblName)

				hiveMetaStore.createTable(t)
			}

		}
	}

	/**
	  * It removes s3 path for existing partitions
	  *
	  * @param s3fs         : FileSystem
	  * @param tablename    : tablename
	  * @param prefix       : partion prefix
	  * @param datalocation : temp data location
	  * @param partcols     : Partitions Columns
	  */

	def s3removedir(dfs: FileSystem, s3fs: FileSystem, tablename: String, prefix: String, datalocation: String, partcols: String): Unit = {
		if (partcols != "") {
			val pathdirs = dfs.globStatus(new Path(datalocation + "/*" * partcols.split('|').length)).map(fs => fs.getPath.toString)
			for (i <- pathdirs) {
				val Array(_, partition) = i.split(tablename, 2)
				val dir = prefix + partition
				s3Remove(s3fs, dir)
			}
		} else
			s3Remove(s3fs, prefix)
	}

	/**
	  * ends job before going to processing state and make job status as succeeded
	  *
	  * @param instanceId          : jobinstanceid
	  * @param LowWatermarkString  : LowWatermarkString
	  * @param HighWatermarkString : HighWatermarkString
	  * @param LowWatermarkTime    : LowWatermarkTime
	  * @param HighWatermarkTime   : HighWatermarkTime
	  */

	def softexit(jc: JobControl, processName: String, instanceId: String, LowWatermarkString: String, HighWatermarkString: String, LowWatermarkTime: String, HighWatermarkTime: String): Unit = {

		log.warn("No New Data from Source, So exiting Grace Fully by adding job controls")

		jc.endJob(instanceId, 2, LowWatermarkTime, HighWatermarkTime, LowWatermarkString, HighWatermarkString)

		log.info(s" $instanceId $LowWatermarkString $HighWatermarkString")

		jc.logInfo(instanceId, s"END $processName", "0".toLong)

		log.info(s"added job controls and now exiting")

		System.exit(0)
	}

	/**
	  * converts from any timzone time stamp to specified time zone
	  *
	  * @param df            : Dataframe
	  * @param exclude       : columns to exclude
	  * @param from_timezone : Timezone of existing
	  * @param to_timezone   : Timezone to be desired
	  * @param filterstring  : add any filters to not to do timezone conversion -- sample -- appid in (21,23)
	  * @return cols          : columns with Timezone conversion
	  */

	def convertDateTimeColsTimezoneFromDF(df: DataFrame, exclude: Seq[String] = Seq(), from_timezone: String = "", to_timezone: String = "", filterstring: String = ""): Array[(String)] = {
		val cols = mutable.ArrayBuffer[(String)]()
		for (column <- df.dtypes) {
			val (col, dataType) = column
			if (!(exclude contains col)) {
				if (sparktoHiveDataType(dataType) == "timestamp" || sparktoHiveDataType(dataType) == "date") {
					cols += (if (to_timezone != "" & to_timezone.toLowerCase() != "utc") {
						if (filterstring != "") {
							s"Case when $filterstring Then " + "from_utc_timestamp(" + "to_utc_timestamp(" + col.toLowerCase() + s",upper('$from_timezone')),upper('$to_timezone')) else " + col.toLowerCase() + " end as " + col
						} else {
							"from_utc_timestamp(" + "to_utc_timestamp(" + col.toLowerCase() + s",upper('$from_timezone')),upper('$to_timezone')) as " + col
						}
					} else {
						if (filterstring != "") {
							s"Case when $filterstring Then " + "to_utc_timestamp(" + col.toLowerCase() + s",upper('$from_timezone')) else " + col.toLowerCase() + " end as " + col
						} else {
							"to_utc_timestamp(" + col.toLowerCase() + s",upper('$from_timezone')) as " + col
						}
					})
				}
				else
					cols += col
			}
		}
		cols.toArray
	}


	/**
	  * Construct Hive Partitions to be used further in addHivePartitions
	  *
	  * @param hiveMetaStore : HiveMetaStoreClient
	  * @param df            : DataFrame from which partition values to be derived from
	  * @param db            : Hive Database Name
	  * @param tbl           : Hive Table Name
	  * @param partCols      : Partitions Columns
	  * @return Hive Partitions that can be used towards addHivePartitions
	  */
	def constructHivePartitions(hiveMetaStore: HiveMetaStoreClient, df: DataFrame, db: String, tbl: String, partCols: Seq[String]): List[(List[String], String)] = {
		val baseLoc = getHiveTableLocation(hiveMetaStore, db, tbl)
		this.constructHivePartitions(df, partCols, baseLoc)
	}


	/**
	  * DEPRECATED: use `constructHivePartitions`
	  * Construct Hive Partitions to be used further in addHivePartitions
	  *
	  * @param df       : DataFrame from which partition values to be derived from
	  * @param partCols : Partition Columns
	  * @param baseLoc  : Base Location of the Hive Table
	  * @return Hive Partitions that can be used towards addHivePartitions
	  */
	@deprecated
	def deprecatedConstructHivePartitions(df: DataFrame, partCols: Seq[String], baseLoc: String): List[(List[String], String)] = {
		val parts = df.selectExpr(partCols.map(x => s"""CAST($x AS String) AS $x"""): _*).distinct.collect
		parts.map { x => val partStr = partCols.map { col => val partValue = x.getAs[String](col); (partValue, col + "=" + partValue) }; (partStr.map(_._1).toList, baseLoc + "/" + partStr.map(_._2).mkString("/")) }.toList
	}

	/**
	  * Construct Hive Partitions to be used further in addHivePartitions
	  *
	  * @param df       : DataFrame from which partition values to be derived from
	  * @param partCols : Partition Columns
	  * @param baseLoc  : Base Location of the Hive Table
	  * @return Hive Partitions that can be used towards addHivePartitions
	  */
	def constructHivePartitions(df: DataFrame, partCols: Seq[String], baseLoc: String): List[(List[String], String)] = {
		val parts = df.selectExpr(partCols.map(x => s"""CAST($x AS String) AS $x"""): _*).distinct.collect
		val partRecords = parts.map(row => {
			val paired = partCols.map(partCol => {
				val partValue = row.getAs[String](partCol)
				(partCol, partValue)
			})
			(paired.map(_._1), paired.map(_._2))
		})
		partRecords.map(tup => {
			val (cols, vals) = tup
			val safeBaseLoc = if (baseLoc.endsWith("/")) baseLoc else s"${baseLoc}/"
			(vals.toList, s"${safeBaseLoc}${FileUtils.makePartName(cols, vals)}")
		}).toList
	}


	/**
	  * Add Partition to Hive Table
	  *
	  * @param hiveMetaStore : HiveMetatore
	  * @param dbName        : Database name
	  * @param tblName       : Table name
	  * @param parts         : Partitions to be added in BULK, it is a seq(tuple), the first value in tuple is the partitions value, second value is the location info
	  *                      for that partition. Ex: for partitions(send_date, country) List((List(20171101, US),"finalLoc/send_date=20171101/country=US"))
	  * @return Unit
	  */
	def addHivePartitions(hiveMetaStore: HiveMetaStoreClient, dbName: String, tblName: String, parts: List[(List[String], String)]): Unit = {

		val table = hiveMetaStore.getTable(dbName, tblName)
		val partitions = for (part <- parts) yield {
			val (p, loc) = part
			val partition = new Partition()
			partition.setDbName(dbName)
			partition.setTableName(tblName)
			val sd = new StorageDescriptor(table.getSd)
			sd.setLocation(loc)
			partition.setSd(sd)
			partition.setValues(p.asJava)
			partition
		}

		hiveMetaStore.add_partitions(partitions.asJava, true, true)

		hiveMetaStore.alter_partitions(dbName, tblName, partitions.asJava)
	}


	/**
	  * Generate Quoted String from a Seq
	  *
	  * @param seq : Seq to generate string from
	  * @return Quoted String
	  */
	def seqToQuotedString(seq: Seq[String]): String = {
		seq.mkString("'", "', '", "'")
	}


	/**
	  * Similar to "hdfs -mv" operation. Moves/Renames HDFS Source to Target Paths
	  *
	  * @param dfc     : FileContext
	  * @param srcPath : Source Path
	  * @param tgtPath : Target Path
	  */
	def hdfsMove(dfc: FileContext, srcPath: String, tgtPath: String): Unit = {
		log.info(s"Moving $srcPath to $tgtPath")
		dfc.rename(new Path(srcPath), new Path(tgtPath), Rename.OVERWRITE)
	}


	/**
	  * Similar to "hdfs dfs -rm -r -skipTrash" operation. Deletes specified HDFS Location
	  *
	  * @param s3fs ; FileSystem
	  * @param path : HDFS Path to be deleted
	  */
	def s3Remove(s3fs: FileSystem, path: String): Unit = {
		if (s3fs.exists(new Path(path))) s3fs.delete(new Path(path), true)
	}


	/**
	  * Similar to "hdfs -mv" operation. Moves/Renames HDFS Source to Target Paths
	  *
	  * @param s3fs    : S3 FileSystem
	  * @param srcPath : Source Path
	  * @param tgtPath : Target Path
	  */
	def s3Move(s3fs: FileSystem, srcPath: String, tgtPath: String): Unit = {
		log.info(s"Moving $srcPath to $tgtPath")
		s3fs.rename(new Path(srcPath), new Path(tgtPath))
	}


	/**
	  * Similar to "hdfs dfs -mv" except if the target path already exists it DOESN'T throw error
	  * rather it deletes Target Path and Renames Source to Target Path
	  *
	  * @param s3fs    : S3 FileSystem
	  * @param srcPath : Source Path
	  * @param tgtPath : Target Path
	  */
	def s3RemoveAndMove(s3fs: FileSystem, srcPath: String, tgtPath: String): Unit = {
		s3Remove(s3fs, tgtPath)
		s3Move(s3fs, srcPath, tgtPath)
	}


	/**
	  * Similar to "hdfs dfs -mv" except if the target path already exists it DOESN'T throw error
	  * rather it deletes Target Path and Renames Source to Target Path
	  *
	  * @param dfs     : FileSystem
	  * @param dfc     : FileContext
	  * @param srcPath : Source Path
	  * @param tgtPath : Target Path
	  */
	def hdfsRemoveAndMove(dfs: FileSystem, dfc: FileContext, srcPath: String, tgtPath: String): Unit = {
		hdfsRemove(dfs, dfc, tgtPath)
		dfs.mkdirs(new Path(tgtPath))
		hdfsMove(dfc, srcPath, tgtPath)
	}


	/**
	  * Similar to "hdfs dfs -rm -r -skipTrash" operation. Deletes specified HDFS Location
	  *
	  * @param dfs  ; FileSystem
	  * @param dfc  : FileContext
	  * @param path : HDFS Path to be deleted
	  */
	def hdfsRemove(dfs: FileSystem, dfc: FileContext, path: String): Unit = {
		if (dfs.exists(new Path(path))) dfc.delete(new Path(path), true)
	}


	/**
	  * Purge HDFS Path based on Retention Policy
	  *
	  * @param dfs            : FileSystem
	  * @param dfc            : FileContext
	  * @param loc            : HDFS Location to be purged
	  * @param rententionDays : Retention Days
	  */
	def purgeHDFSPath(dfs: FileSystem, dfc: FileContext, loc: String, rententionDays: String): Unit = {
		dfs.listStatus(new Path(loc))
			.filter(_.getPath.toString.split("/").last < rententionDays)
			.map(_.getPath.toString).foreach(hdfsRemove(dfs, dfc, _))
	}


	/**
	  * Creates RDD CheckPoint to cut down Lineage Graph and frees up Driver's Memory
	  *
	  * Note: CheckPoint writes entire Serialized RDD to the specified Checkpoint Directory, please be aware of the RDD size
	  *
	  * Additional Advantage: If the some of the executors get's Lost/Preempted after the checkpoint, it reads the RDD
	  * from disk, saves time without re-computing it
	  *
	  * @param spark : SparkSession
	  * @param df    : DataFrame to be CheckPointed
	  * @param msg   : Message to be displayed for the action Count
	  * @return New Checkpointed DataFrame
	  */
	def createCheckPoint(spark: SparkSession, jc: JobControl, df: DataFrame, msg: String = ""): DataFrame = {
		df.rdd.checkpoint
		val dfCount = df.rdd.count

		if (msg != "") {
			log.info(s"Logging Count - $msg - $dfCount")
			jc.logInfo(spark.conf.get("instanceId"), msg, dfCount)
		}
		else log.info(s"Checkpoint Count - $dfCount")

		spark.createDataFrame(df.rdd, df.schema)
	}


	/**
	  * Returns DataFrame with new nullability constraints from given schema
	  *
	  * @param spark  = source Spark Session
	  * @param df     = source DataFrame
	  * @param schema = schema with nullability constraints to be enforced on source DataFrame
	  * @return Returns DataFrame with new nullability constraints from given schema
	  */
	def enforceSchemaNullability(spark: SparkSession, df: DataFrame, schema: StructType): DataFrame = {
		//ensures schema field names are only entered once
		val colCount: Int = schema.map(x => (x.name, x.nullable)).groupBy(_._1).mapValues(_.length).values.reduce(math.max)
		assert(colCount == 1, "Enforced schema has more than one field with the same name!")

		val nullabilityMap: Map[String, Boolean] = schema.map(x => (x.name.toLowerCase, x.nullable)).toMap
		val newFields = df.schema.map(x => x.copy(nullable = nullabilityMap(x.name.toLowerCase)))
		val newSchema = StructType(newFields)
		spark.createDataFrame(df.rdd, newSchema)
	}


	/**
	  * Returns DataFrame with new column types casted from given schema
	  *
	  * @param df     = source DataFrame
	  * @param schema = new schema to which the source DataFrame will have its columns casted to align with
	  * @return Returns DataFrame with new column types casted from given schema
	  */
	def enforceSchemaTypes(df: DataFrame, schema: StructType): DataFrame = {
		//ensures schema field names are only entered once
		val colCount: Int = schema.map(x => (x.name, x.dataType)).groupBy(_._1).mapValues(_.length).values.reduce(math.max)
		assert(colCount == 1, "Enforced schema has more than one field with the same name!")

		val typeMap: Map[String, DataType] = schema.map(x => (x.name.toLowerCase, x.dataType)).toMap
		df.columns.foldLeft(df) { (z, col) => z.withColumn(col, z(col).cast(typeMap(col.toLowerCase))) }
	}

	/**
	  * Returns unit with result
	  *
	  * @param dfs                   = FileSystem
	  * @param s3Bucket              = s3bucket
	  * @param targetDataTmpLocation = targetDataTempLocation
	  * @param targetPartCols        = target partition columns
	  * @param SplitString           = Split Path based on tablename
	  * @param prefix                = s3prefix from s3path
	  * @param localdatalocation     = location to copy from local to s3
	  */

	def multis3objectdelete(dfs: FileSystem, s3Bucket: String, targetDataTmpLocation: String, targetPartCols: Seq[String], SplitString: String, prefix: String, tgtDataLocation: String, localdatalocation: String): Unit = {

		val tmpDirs = dfs.globStatus(new Path(targetDataTmpLocation + "/*" * targetPartCols.length)).map(fs => fs.getPath.toString)

		val keys = new ArrayBuffer[String]()

		val s3 = AmazonS3ClientBuilder.defaultClient()

		var dir = ""

		for (i <- tmpDirs) {
			if (targetPartCols.nonEmpty) {
				val Array(_, partition) = i.split(SplitString)
				dir = prefix + partition
			} else {
				dir = prefix
			}

			val req = new ListObjectsV2Request().withBucketName(s3Bucket).withPrefix(dir)
			var result = new ListObjectsV2Result

			do {
				result = s3.listObjectsV2(req)
				keys ++= result.getObjectSummaries.map(x => x.getKey)
				req.setContinuationToken(result.getNextContinuationToken)
			} while (result.isTruncated)
		}

		Seq("/bin/sh", "-c", s"aws s3 cp $localdatalocation `echo $tgtDataLocation | sed 's/s3a:/s3:/g' ` --recursive").!!.trim

		log.info(s"completed copy Process to s3 path for: $SplitString")

		if (keys.nonEmpty) {
			if (keys.length > 950) {
				val newkeys = keys.sliding(500, 500).toList
				for (key <- newkeys) {
					print(key.length)
					s3.deleteObjects(new DeleteObjectsRequest(s3Bucket).withKeys(key: _*))
				}
			} else
				s3.deleteObjects(new DeleteObjectsRequest(s3Bucket).withKeys(keys: _*))
		}
		log.info(s"completed S3 objects delete Process for: $SplitString")
	}

	/**
	  * Returns DataFrame with updated rows
	  *
	  * DataFrames cannot be defined from others using a Spark SQL UPDATE statement.  This function allows
	  * the caller to supply an UPDATE-like statement and the DataFrame to be updated and will merge the updated rows
	  * returned by the query with those not selected for update.
	  *
	  * @param spark      = SparkSession object
	  * @param df         = DataFrame from which new, updated rows will be generated from
	  * @param dfViewName = the view created for the df
	  * @param query      = String which will query the passed df to generate new rows that can be unioned with the old, unmodified rows
	  * @return Returns DataFrame with updated rows
	  */
	def updateDataFrame(spark: SparkSession, jc: JobControl, df: DataFrame, dfViewName: String, query: String, msg: String = ""): DataFrame = {

		// Ensure query contains the substring "{SOURCE_DF}"
		val queryDFKeyword = "{SOURCE_DF}"
		assert(query.contains(queryDFKeyword), s"Updating query does not contain $queryDFKeyword!")

		// Add a unique row identifier to the DataFrame being updated and create a corresponding view
		val rowIdColName = "tmpRowId"
		val oldDFWithIds = df.withColumn(rowIdColName, monotonically_increasing_id())

		// Create view atop the source DataFrame with a rowid field
		val oldViewName = "oldDFWithIds_VIEW"
		oldDFWithIds.createOrReplaceTempView(oldViewName)

		// Inject the view with rowids into the supplied query
		val viewAdjustedQuery = query.replaceAllLiterally(queryDFKeyword, oldViewName)

		// Get updated rows
		val newRowsWithTMPCols = spark.sql(viewAdjustedQuery)

		// Find new "_TMP" columns
		val tmpRegEx = "\\_TMP$".r
		val tmpCols = newRowsWithTMPCols.columns.filter(_.endsWith("_TMP"))
		assert(tmpCols.length > 0, s"No new columns defined in the update to $dfViewName!")

		// Rename the TMP columns in the updated rows to their proper names
		var newRowsWithRenamedTMPCols = tmpCols.foldLeft(newRowsWithTMPCols) {
			(z, col) => {
				val cleanedCol = tmpRegEx.replaceAllIn(col, "")
				z.withColumn(cleanedCol, z(col)).drop(col)
			}
		}

		val checkExtraCols = newRowsWithRenamedTMPCols.columns.map(_.toLowerCase).toSet -- oldDFWithIds.columns.map(_.toLowerCase).toSet

		if (checkExtraCols.nonEmpty) {
			newRowsWithRenamedTMPCols = newRowsWithRenamedTMPCols.drop(checkExtraCols.toList: _*)
		}

		// Enforce the old schema on the new df
		val updatedNullabilityRows = enforceSchemaNullability(spark, newRowsWithRenamedTMPCols, oldDFWithIds.schema)
		val newDFWithIds = enforceSchemaTypes(updatedNullabilityRows, oldDFWithIds.schema)

		val newViewName = "newDFWithIds_VIEW"
		newDFWithIds.createOrReplaceTempView(newViewName)

		newDFWithIds.persist
		val updatedCount = newDFWithIds.count

		if (msg != "") {
			log.info(s"Logging Count - $msg - $updatedCount")
			jc.logInfo(spark.conf.get("instanceId"), msg, updatedCount)
		}
		else log.info(s"UPDATED ROWS Count - $updatedCount")

		val cols = oldDFWithIds.columns
			.filterNot(_ == rowIdColName)
			.map(x => s"CASE WHEN B.$rowIdColName IS NOT NULL THEN B.$x ELSE A.$x END AS $x")
			.mkString(",")

		val joinquery = s"SELECT $cols FROM $oldViewName A LEFT JOIN $newViewName B ON A.$rowIdColName = B.$rowIdColName"

		val finalDF = spark.sql(joinquery).repartition(50)
		finalDF.persist
		val totalCount = finalDF.count
		log.info(s"TOTAL ROWS Count After the Update Operation: $totalCount")

		newDFWithIds.unpersist
		finalDF
	}


	/**
	  * Transpose Columns to Rows
	  *
	  * Ex:
	  *
	  * Input DF:
	  * ----------------------
	  * year|visits|inquiries
	  * ----------------------
	  * 2016|250000|10000
	  * 2017|500000|20000
	  *
	  * Output DF:
	  * -----------------------------
	  * year|column_name|column_value
	  * -----------------------------
	  * 2016|visits     |250000
	  * 2016|inquiries  |10000
	  * 2017|visits     |500000
	  * 2017|inquiries  |20000
	  *
	  * @param transDF : DF to be transposed.
	  * @param transBy : Transpose By Columns
	  * @return Transposed DF
	  */
	def transposeUDF(transDF: DataFrame, transBy: Seq[String]): DataFrame = {
		val (cols, types) = transDF.dtypes.filter { case (c, _) => !transBy.contains(c) }.unzip
		require(types.distinct.length == 1)

		val kvs = explode(array(
			cols.map(c => struct(lit(c).alias("column_name"), col(c).alias("column_value"))): _*
		))

		val byExprs = transBy.map(col)

		transDF
			.select(byExprs :+ kvs.alias("_kvs"): _*)
			.select(byExprs ++ Seq(col("_kvs.column_name"), col("_kvs.column_value")): _*)
	}

	/**
	  * Deletes S3 objects based on batchid and keeps current Hive associated location and last batchid data for a given one partition value
	  *
	  * @param hiveMetaStore      : HiveMetastore -- to check current associated partition/table location
	  * @param targetdatalocation : s3 targetdataloacation along with batchid -- ex:s3://<bucketname>/<dbname>/<tablename>
	  * @param spark              : Spark Session
	  * @param targettable        : targettable - along with dbname
	  * @param partcolumns        : partitioncolumns as array or Seq
	  * @param datadf             : actual data df for write to s3 path
	  * @param partsdf            : partition values data df for adding partitions, if you dont have partitions or dont have separate dataframe for partvaleus, just pass samedf- it does not harm in any way
	  * @param dataformat         : which format that data want to be in target ex.. CSV,Parquet,ORC
	  * @param env                : environment
	  * @param objectdelete       : tells process to delete objects
	  * @param deletelog          : to log objects which got deleted
	  * @param noofcopies         : Number of copies to keep excluding current hive associated data 1- previous copy of data 0 - no backup copies
	  */

	def s3objectwritebatchid(hiveMetaStore: HiveMetaStoreClient, targetdatalocation: String, spark: SparkSession, targettable: String, partcolumns: Seq[String], datadf: DataFrame, partsdf: DataFrame, dataformat: String, env: String, objectdelete: Boolean = true, objectdeletelog: Boolean = true, noofcopies: Int = 1): Unit = {

		import spark.implicits._

		val batchid = DateTime.now.withZone(DateTimeZone.UTC).toString("yyyyMMddHHmm")

		val datalocation = targetdatalocation + s"/$batchid"

		val dbName = targettable.split('.')(0)

		val tableName = targettable.split('.')(1)

		log.info(s"started writing to S3 path $datalocation  for: $targettable")

		saveDataFrameToDisk(datadf, SaveMode.Overwrite, dataformat, datalocation, partcolumns.toArray)

		log.info(s"completed writing to S3 path $datalocation  for: $targettable")

		if (partcolumns.nonEmpty) {

			addHivePartitions(hiveMetaStore, dbName, tableName, constructHivePartitions(partsdf, partcolumns, datalocation))
		} else {
			alterTableLocation(hiveMetaStore, dbName, tableName, datalocation)
		}

		log.info(s"completed added/alter partitions for: $targettable")

		log.info(s"started S3 objects delete Process for: $targettable")

		if (objectdelete) {
			val s3 = AmazonS3ClientBuilder.defaultClient()

			val s3targeturi = new AmazonS3URI(s"$targetdatalocation".replace("s3a:", "s3:"))

			val s3bucket = s3targeturi.getBucket

			val s3_prefix = s3targeturi.getKey

			val s3prefix = if (s3_prefix.endsWith("/")) {
				s"$s3_prefix"
			} else {
				s"$s3_prefix/"
			}

			val req = new ListObjectsV2Request().withBucketName(s3bucket).withPrefix(s3prefix)

			var result = new ListObjectsV2Result

			val keys = new ArrayBuffer[String]()

			do {
				result = s3.listObjectsV2(req)
				keys ++= result.getObjectSummaries.map(x => x.getKey)
				req.setContinuationToken(result.getNextContinuationToken)
			} while (result.isTruncated)

			val s3partlocsdf = keys.toDF().createOrReplaceTempView("s3partlocs")

			val partitionlist = hiveMetaStore.listPartitionSpecs(dbName, tableName, 32767)

			val hivepartlocsdf = if (partcolumns.nonEmpty) {
				partitionlist.toPartitionSpec.asScala.toList(0).getPartitionList.getPartitions.map(x => x.getSd.getLocation).toDF().createOrReplaceTempView("hivepartlocs")
			} else {
				List(hiveMetaStore.getTable(targettable.split('.')(0), targettable.split('.')(1)).getSd.getLocation).toDF().createOrReplaceTempView("hivepartlocs")
			}

			spark.udf.register("arrayreverse", udf((xs: Seq[String]) => Try(xs.reverse).toOption))

			var multipartcolumnssplitstring = ""

			var partcollength: Int = 0

			if (partcolumns.nonEmpty) {

				multipartcolumnssplitstring = (for ((part, i) <- partcolumns.zipWithIndex) yield s"arrayreverse(split(value,'/'))[" + (i + 1) + "]").mkString(",")

				partcollength = partcolumns.length + 1

			}

			spark.sql(s"REFRESH TABLE $targettable")

			val objectstobedeleted: List[String] = if (partcolumns.nonEmpty) {
				spark.sql(
					s"""
					   |Select value
					   |from
					   |(select
					   |value,
					   |rank() over (partition by $multipartcolumnssplitstring  order by cast(arrayreverse(split(value,'/'))[$partcollength] as bigint) desc ) rn
					   |from
					   |s3partlocs
					   |where
					   |concat("$s3bucket","/",substring_index(value,'/',size(split(value,'/'))-1)) not in (select split(value,'://')[1] from hivepartlocs)
					   |) t
					   |where rn > $noofcopies """.stripMargin).select("value").collect().map { row => row.toString().replaceAll("[\\[\\]]", "") }.toList
			} else {
				spark.sql(
					s"""
					   |Select value
					   |from
					   |(select
					   |value,
					   |rank() over (order by cast(arrayreverse(split(value,'/'))[1] as bigint) desc) rn
					   |from
					   |s3partlocs
					   |where
					   |concat("$s3bucket","/",substring_index(value,'/',size(split(value,'/'))-1)) not in (select split(value,'://')[1] from hivepartlocs)
					   |) t
					   |where rn > $noofcopies """.stripMargin).select("value").collect().map { row => row.toString().replaceAll("[\\[\\]]", "") }.toList
			}

			if (objectdeletelog) {

				val objdel = objectstobedeleted.toDF().createOrReplaceTempView("objectsdelete")

				spark.sql(s"""select "$targettable" as tablename ,value as objectname,"$batchid" as batchid ,current_date as partdate from objectsdelete""").coalesce(1).write.mode(SaveMode.Append).format("parquet").partitionBy("partdate").save(s"s3a:/.../objectdeletelog/")

				log.info("logging completed object deletes into s3 path for debugging")
			}

			if (objectstobedeleted.nonEmpty) {
				if (objectstobedeleted.length > 950) {
					val newkeys = objectstobedeleted.sliding(500, 500).toList
					for (key <- newkeys) {
						print(key.length)
						s3.deleteObjects(new DeleteObjectsRequest(s3bucket).withKeys(key: _*))
					}
				} else
					s3.deleteObjects(new DeleteObjectsRequest(s3bucket).withKeys(objectstobedeleted: _*))
			}

			log.info(s"completed S3 objects delete Process for: $targettable")

		}
	}
}
