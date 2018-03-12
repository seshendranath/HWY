package com.homeaway.analyticsengineering.encrypt.main.loader

import java.text.SimpleDateFormat
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.collection.mutable.ListBuffer
import java.util.concurrent.Executors
import scala.concurrent._
import java.util.{Calendar, Properties}
import com.homeaway.analyticsengineering.encrypt.secret.Decrypt

/**
  * PLEASE NOTE: The dataframes being returned by the functions in this class are created using different
  * SparkSession. 
  *
  * If you'd like to create temp views on top of these dataframes, 
  * please use df.createOrReplaceGlobalTempView("df") as these global temp views can be accessed across sessions.
  *
  * All the global temp views will be under Spark's global_temp database.
  *
  * To access these Global Temp Views in SQL API see the below example:
  *
  * Ex:
  * val df = edwLoader.getData("select * from dw.dbo.brand")
  * df.createOrReplaceGlobalTempView("df")
  * spark.sql("select * from global_temp.df").show(false)
  *
  */
final class EDWLoader(ospark: SparkSession, appName: String = "ae", rdb: String = "sqlserver") {

	private val driver = ospark.conf.get(s"$rdb.driver")
	private val spark = Decrypt.getSparkSession(ospark, appName, rdb)


	/**
	  * Get's the Primary Key (Ignore's Data Columns) of the table in SQL Server - Used for partitioning the dataset
	  *
	  * @param db  : SQL Server's Database
	  * @param tbl : SQL Server's Table
	  * @return Primary Key to be used for partitioning the dataset
	  */
	def getPrimaryKey(db: String, tbl: String): String = {
		val query =
			s"""
			   |SELECT column_name as PRIMARYKEYCOLUMN
			   |FROM $db.INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
			   |INNER JOIN
			   |    $db.INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
			   |          ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' AND
			   |             TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME AND
			   |             KU.table_name='$tbl'
     """.stripMargin

		getData(query).collect().filterNot(_ (0).toString.toLowerCase contains "date")(0)(0).toString.toLowerCase
	}


	/**
	  * Fetches data from SQL Server to Spark
	  *
	  * @param edwSqlText : Query to be fetched
	  * @return DataFrame
	  */
	def getData(edwSqlText: String): DataFrame = {
		spark
			.read
			.option("user", spark.conf.get("user"))
			.option("password", spark.conf.get("password"))
			.option("url", spark.conf.get("url"))
			.option("driver", driver)
			.format("jdbc")
			.option("dbtable", s"($edwSqlText) as src")
			.load
	}


	/**
	  * Fetches Data from SQL Server using Multiple Threads
	  *
	  * @param sqlQuery         : Query to be fetched
	  * @param numPartitions    : Number of threads
	  * @param partitionColName : partitioncolumnname
	  * @return DataFrame
	  */
	def getDataParallelly(sqlQuery: String, numPartitions: Int = 20, partitionColName: String = ""): DataFrame = {

		var query = sqlQuery.replaceAll("\\s+", " ")

		val (db, schema, table) = """(?i) FROM ([\w.]+)""".r.findAllIn(query).matchData.map(_.group(1)).toList.head.split("\\.") match {
			case Array(a, b, c) => (a, b, c)
		}

		val partitionColumnName = if (partitionColName != "") {
			partitionColName
		} else {
			getPrimaryKey(db, table)
		}

		val boundQueryDf = getData(s"SELECT MIN($partitionColumnName) min, MAX($partitionColumnName) max FROM $db.$schema.$table").collect()(0)

		val (lowerBound, upperBound) = (boundQueryDf.get(0).toString.toLong, boundQueryDf.get(1).toString.toLong)

		val cols = """(?i)SELECT (.*?) FROM """.r.findAllIn(query).matchData.map(_.group(1)).toList.head.split(",").map(_.trim).map(_.toLowerCase).toSet

		if (!cols.contains(partitionColumnName) && cols.mkString(",") != "*" && !query.contains(".*") && !query.contains("*")) {
			query = query.replaceFirst("(?i)SELECT ", "SELECT " + partitionColumnName + ", ")
		}

		spark.read
			.format("jdbc")
			.option("user", spark.conf.get("user"))
			.option("password", spark.conf.get("password"))
			.option("url", spark.conf.get("url"))
			.option("driver", driver)
			.option("partitionColumn", partitionColumnName)
			.option("lowerBound", lowerBound)
			.option("upperBound", upperBound)
			.option("numPartitions", numPartitions)
			.option("dbtable", String.format("(%s) as src", query))
			.load
	}

	/**
	  * Fetches Data from Sql Server parallelly using Sacala Futures and Multi Threading
	  *
	  * @param tbl       : Table/View Name
	  * @param dateCol   : Data Column on which View/Table is partitioned/Indexed on
	  * @param startDate : Start Date of the Data
	  * @param endDate   : End Date of the Data
	  * @return Dataframe
	  */

	def getDataParallelly(tbl: String, dateCol: String, startDate: String, endDate: String): DataFrame = {

		val parsedDate = new SimpleDateFormat("yyyy-MM-dd")

		val cal = Calendar.getInstance()
		cal.setTime(parsedDate.parse(startDate))

		val dates = new ListBuffer[(String, String)]()

		val e = Calendar.getInstance()
		e.setTime(parsedDate.parse(endDate))
		e.add(Calendar.MONTH, 1)

		while (parsedDate.format(e.getTime) > parsedDate.format(cal.getTime)) {

			cal.set(Calendar.DAY_OF_MONTH, 1)
			val startOfMonth = parsedDate.format(cal.getTime)

			cal.set(Calendar.DAY_OF_MONTH, cal.getActualMaximum(Calendar.DAY_OF_MONTH))
			val endOfMonth = parsedDate.format(cal.getTime)

			dates += ((startOfMonth, endOfMonth))

			cal.add(Calendar.MONTH, 1)
		}

		val dummy = getData(s"SELECT * from $tbl WHERE 1 != 1")
		var finalDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], dummy.schema)

		val executorService = Executors.newFixedThreadPool(50)
		val executionContext = ExecutionContext.fromExecutorService(executorService)

		def go(implicit ec: ExecutionContext) = {
			val F: Seq[Future[DataFrame]] = for ((sDate, eDate) <- dates.toList) yield {
				Future {
					val data = getData(s"SELECT * from $tbl WHERE $dateCol >= '$sDate' AND $dateCol <= '$eDate'")
					data.persist
					data.count
					data
				}
			}
			Future.sequence(F).map {
				results => finalDF = results.reduce(_ union _)
			}.recover {
				case error => error.printStackTrace()
			}
		}

		Await.result(go(executionContext), Duration.Inf)

		executorService.shutdown()
		executionContext.shutdown()

		finalDF
	}

	/**
	  * Fetches Data from SQL Server using Multiple Threads
	  *
	  * @param sqlQuery         : Query to be fetched
	  * @param numPartitions    : Number of threads
	  * @param partitionColName : partitioncolumnname
	  * @param incrBound        : to determine boundaries from incremental data
	  * @return DataFrame
	  */

	def getDataParallelly(sqlQuery: String, numPartitions: Int, partitionColName: String, incrBound: Boolean): DataFrame = {

		val boundquery = sqlQuery.split("(?i) from ")(1)

		val boundQueryDf = getData(s"SELECT MIN($partitionColName) min, MAX($partitionColName) max FROM $boundquery").collect()(0)

		val (lowerBound, upperBound) = (boundQueryDf.get(0).toString.toLong, boundQueryDf.get(1).toString.toLong)

		spark.read
			.format("jdbc")
			.option("user", spark.conf.get("user"))
			.option("password", spark.conf.get("password"))
			.option("url", spark.conf.get("url"))
			.option("driver", driver)
			.option("partitionColumn", partitionColName)
			.option("lowerBound", lowerBound)
			.option("upperBound", upperBound)
			.option("numPartitions", numPartitions)
			.option("dbtable", String.format("(%s) as src", sqlQuery))
			.load
	}

	def writeData(df: DataFrame, db: String, tbl: String): Unit = {
		val props = new Properties()
		props.put("user", spark.conf.get("user"))
		props.put("password", spark.conf.get("password"))
		props.put("driver", driver)
		val url = spark.conf.get("url")

		df.write.mode(SaveMode.Append).jdbc(url + ";database=" + db, tbl, props)
	}

}
