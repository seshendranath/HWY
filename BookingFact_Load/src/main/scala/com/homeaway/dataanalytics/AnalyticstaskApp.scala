package com.homeaway.dataanalytics

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.ImportResource
import com.homeaway.dataanalytics.encrypt.main.utilities.JobControl
import org.apache.spark.sql.SparkSession
import lombok.extern.slf4j.Slf4j
import org.apache.hadoop.fs.{FileContext, FileSystem}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.log4j.{Level, Logger}
import org.springframework.context.ApplicationContext
import org.springframework.context.support.ClassPathXmlApplicationContext

/*
 * Main class and entry point to the application
 */

@SpringBootApplication
@ImportResource(Array("classpath:taskContext.xml"))
@Slf4j
private[dataanalytics] class AnalyticstaskApp

object AnalyticstaskApp extends App {

	private val log = Logger.getLogger(getClass)
	log.setLevel(Level.toLevel("Info"))

	val context: ApplicationContext = new ClassPathXmlApplicationContext("sparkContext.xml")
	lazy val spark: SparkSession = context.getBean("sparkSession").asInstanceOf[SparkSession]

	lazy val dfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
	lazy val dfc = FileContext.getFileContext(spark.sparkContext.hadoopConfiguration)
	lazy val hiveMetaStore = new HiveMetaStoreClient(new HiveConf())

	spark.sparkContext.setLogLevel("Warn")

	log.info("=" * 100 + " " + spark.sparkContext.applicationId + " " + "=" * 100)

	lazy val jc = new JobControl(spark)
	lazy val jobId = jc.getJobId(spark.conf.get("objectName"), spark.conf.get("processName"))
	lazy val (instanceId, lsr) = jc.startJob(jobId)

	val s = System.nanoTime()

	try {

		spark.conf.set("jobId", jobId)
		spark.conf.set("instanceId", instanceId)

		val configuration: Array[Object] = Array(classOf[AnalyticstaskApp])
		SpringApplication.run(configuration, args)
	}
	catch {
		case e: Exception => errorHandler(e)
		case er: Error => errorHandler(er)
	}
	finally {
		spark.stop()
	}

	val e = System.nanoTime()
	val totalTime = (e - s) / (1e9 * 60)
	log.info("Total Elapsed time: " + f"$totalTime%2.2f" + " mins")


	def errorHandler(e: Throwable): Unit = {
		log.error("Something went WRONG during the run")
		log.error(e.printStackTrace())
		jc.endJob(instanceId, -1)
		System.exit(1)
	}

}