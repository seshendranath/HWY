package com.homeaway.analyticsengineering.task

/**
  * Created by aguyyala on 10/19/17.
  */


import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.temporal.ChronoUnit
import java.util.Calendar
import java.util.concurrent.Executors
import com.homeaway.analyticsengineering.AnalyticsTaskApp._
import com.homeaway.analyticsengineering.encrypt.main.utilities.Utility
import com.homeaway.analyticsengineering.encrypt.main.loader.EDWLoader
import org.apache.spark.sql.{DataFrame, SaveMode}
import com.amazonaws.services.s3.model.GetObjectRequest
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.io.Source


class DataPush extends Utility {

	def run(): Unit = {

		val edwLoader = new EDWLoader(spark)


		try {

			jc.logInfo(instanceId, s"START DataPush Process", -1)

			val db = spark.conf.get("db")
			val baseLoc = spark.conf.get("baseLoc")
			val format = spark.conf.get("format")

			val parsedDate = new SimpleDateFormat("yyyy-MM-dd")

			val startDate = "2016-01-01"

			val cal: Calendar = Calendar.getInstance()
			val endDate: String = parsedDate.format(cal.getTime)

			val sd: LocalDate = LocalDate.parse(startDate)
			val ed: LocalDate = LocalDate.parse(endDate)
			val numOutputFiles: Int = ChronoUnit.MONTHS.between(sd, ed).toInt

			val s3Object = s3.getObject(new GetObjectRequest(conf("s3ProjectFilesBucket"), conf("dataFile")))
			val tblData = Source.fromInputStream(s3Object.getObjectContent).getLines()

			val tbls = new mutable.LinkedHashMap[String, Array[String]]()

			tblData.foreach {tbl =>
				val data = tbl.split(",")
				tbls += (data.head -> data.tail)
			}

			val executorService  = Executors.newFixedThreadPool(5)
			val executionContext = ExecutionContext.fromExecutorService(executorService)

			def go(implicit ec: ExecutionContext) = {
				val dfs = for (x <- tbls.keys) yield {
					val df = Future {
						edwLoader.getDataParallelly(tbls(x)(0), tbls(x)(1), tbls(x)(2), endDate)
					}
					(x, df)
				}

				val finalDfs = dfs.toArray

				val dataframes: Seq[DataFrame] = Await.result(Future.sequence(finalDfs.map(_._2)), Duration.Inf)

				finalDfs.map(_._1) zip dataframes
			}

			val dfMap = go(executionContext)

			dfMap.foreach { case (x, df) =>
				df.repartition(numOutputFiles).write.mode(SaveMode.Overwrite).format(format).save(s"$baseLoc/$x")
				checkAndCreateHiveDDL(hiveMetaStore, db, x, format, s"$baseLoc/$x", getColsFromDF(df))
			}

			executorService.shutdown()
			executionContext.shutdown()


			jc.endJob(instanceId, 1)
		}
		finally {

		}

	}

}
