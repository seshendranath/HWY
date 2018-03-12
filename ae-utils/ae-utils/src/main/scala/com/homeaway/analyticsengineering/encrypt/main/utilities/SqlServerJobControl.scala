package com.homeaway.analyticsengineering.encrypt.main.utilities

/**
  * Created by aguyyala on 6/21/17.
  */


import org.apache.spark.sql.SparkSession
import scala.collection.mutable

final class SqlServerJobControl(spark: SparkSession) extends Utility {

	private val sqlServer = new SqlServer(spark)

	private val db = "DW_CONFIG"
	private val schema = "EAGLE"
	private val jobTable = "JOB"
	private val etlControlTable = "ETLCONTROL"
	private val etlControlLogTable = "ETLCONTROLLOG"


	/**
	  * Checks whether the Job is Running or Not
	  *
	  * @param jobId : JobId
	  * @return true if Running else flase
	  */
	def isRunning(jobId: Int): Boolean = {
		var isRunning = true
		val query =
			s"""
			   |SELECT COUNT(*) AS isRunning
			   |FROM $db.$schema.$etlControlTable
			   |WHERE jobId = $jobId AND statusFlag = 0
		""".stripMargin

		val rs = sqlServer.executeQuery(query)

		while (rs.next()) {
			isRunning = if (rs.getInt("isRunning") > 0) true else false
		}

		isRunning
	}


	/**
	  * Fetches the Actove Flag of the Job
	  *
	  * @param jobId : Job Id
	  * @return Active Flag (true or false)
	  */
	def getActiveFlag(jobId: Int): Boolean = {

		var activeFlag = true

		val query =
			s"""
			   |SELECT activeFlag
			   |FROM $db.$schema.$jobTable
			   |WHERE jobId = $jobId
		""".stripMargin

		val rs = sqlServer.executeQuery(query)

		while (rs.next()) {
			activeFlag = rs.getBoolean("activeFlag")
		}

		activeFlag
	}


	/**
	  * Fetches last Successful job's instance details
	  *
	  * @param jobId : Job Id's instance to be fetched
	  * @return Last Successful Instance details of a job in a Map
	  */
	def getLastSuccessfulRun(jobId: Int): Map[String, String] = {

		val query =
			s"""
			   |SELECT TOP 1  JobId
			   |             ,jobStartDate AS lastSuccessfulJobStartDate
			   |             ,waterMarkStartTime AS lastSuccessfulWaterMarkStartTime
			   |             ,CAST(waterMarkStartTime AS DATE) AS lastSuccessfulWaterMarkStartDate
			   |             ,waterMarkEndTime AS  lastSuccessfulWaterMarkEndTime
			   |             ,CAST(waterMarkEndTime AS DATE) AS  lastSuccessfulWaterMarkEndDate
			   |             ,intWaterMark AS lastSuccessfulIntWaterMark
			   |             ,stringWaterMark AS lastSuccessfulStringWaterMark
			   |FROM $db.$schema.$etlControlTable
			   |WHERE JobId = $jobId AND statusFlag = 1
			   |ORDER BY JobStartTime DESC
		""".stripMargin

		val rs = sqlServer.executeQuery(query)

		val rsmd = rs.getMetaData
		val columnsNumber = rsmd.getColumnCount

		val result = mutable.Map[String, String]()

		while (rs.next()) {
			for (i <- 1 to columnsNumber) {
				val columnValue = rs.getString(i)
				if (columnValue != null) result(rsmd.getColumnName(i)) = columnValue.toString
			}
		}

		result.toMap
	}


	/**
	  * Starts Job Instance
	  *
	  * @param jobId : Job Id to be instantiated
	  * @return Last Successful Instance Details
	  */
	def startJob(jobId: Int): (String, Map[String, String]) = {

		val lsr = getLastSuccessfulRun(jobId)

		val instanceId = generateTimeUUID

		if (!getActiveFlag(jobId)) {
			log.warn("JOB IS CURRENTLY NOT ACTIVE, EXITING GRACEFULLY")
			System.exit(0)
		}

		val query =
			s"""
			   |INSERT INTO $db.$schema.$etlControlTable(jobId, instanceId)
			   |VALUES ($jobId, '$instanceId')
		""".stripMargin

		sqlServer.executeUpdate(query)

		(instanceId, lsr)

	}


	/**
	  * End Job's Instance
	  *
	  * @param instanceId         : Instance Id to END
	  * @param statusFlag         : statusFlag 1 - SUCCEEDED, 0 - Running, -1 - FAILED
	  * @param waterMarkStartTime : ETL Start Time
	  * @param waterMarkEndTime   : ETL End Time
	  * @param intWaterMark       : int watermark of source
	  * @param stringWaterMark    : string watermark of source
	  * @return 1
	  */
	def endJob(instanceId: String, statusFlag: Int, waterMarkStartTime: String = "9999-12-31",
	           waterMarkEndTime: String = "9999-12-31", intWaterMark: Long = -1, stringWaterMark: String = "-1"): Int = {

		val query =
			s"""
			   |UPDATE DW_CONFIG.EAGLE.ETLCONTROl
			   |SET  statusFlag = $statusFlag
			   |    ,WaterMarkStartTime = '$waterMarkStartTime'
			   |    ,WaterMarkEndTime = '$waterMarkEndTime'
			   |    ,IntWaterMark = $intWaterMark
			   |    ,StringWaterMark = '$stringWaterMark'
			   |    ,JobEndTime = GETDATE()
			   |WHERE InstanceId = '$instanceId'
		""".stripMargin

		sqlServer.executeUpdate(query)

	}


	/**
	  * Inserts Job to Job Table
	  *
	  * @param objectName  : Object Name
	  * @param processName : Process Name
	  */
	def insertJob(objectName: String, processName: String): Int = {

		val query =
			s"""
			   |INSERT INTO  $db.$schema.$jobTable(ProcessName, ObjectName, JobType, JobGroup, Description, ActiveFlag)
			   |VALUES('$processName', '$objectName', '${spark.conf.get("jobType")}', '${spark.conf.get("jobGroup")}', '${spark.conf.get("description")}', 1)
		""".stripMargin

		sqlServer.executeUpdate(query)
	}


	/**
	  * Fetches Job ID from Job Table
	  *
	  * @param objectName  : Object Name
	  * @param processName : Process Name
	  * @return Job Id (return -1 if Job doesn't exist)
	  */
	def getJobId(objectName: String, processName: String): Int = {

		var jobId = -1

		val query =
			s"""
			   |SELECT JobId
			   |FROM $db.$schema.$jobTable
			   |WHERE objectName = '$objectName' AND processName = '$processName'
		""".stripMargin

		val rs = sqlServer.executeQuery(query)

		while (rs.next()) {
			jobId = rs.getInt("JobId")
		}

		if (jobId == -1) {
			log.warn("Job NOT FOUND in the Job Table, Adding it now.")
			insertJob(objectName: String, processName: String)
			jobId = getJobId(objectName, processName)

		}

		jobId
	}


	/**
	  * Logs Info to the ETLControlLog Table
	  *
	  * @param jobId             : Job Id
	  * @param instanceId        : Instance Id
	  * @param processActionName : Message of the Action
	  * @param recordCount       : Record Count
	  * @return 1
	  */
	def logInfo(jobId: Int, instanceId: String, processActionName: String, recordCount: Long): Int = {

		val query =
			s"""
			   |INSERT INTO $db.$schema.$etlControlLogTable(JobId, InstanceId, ProcessActionName, RecordCount)
			   |VALUES ($jobId, '$instanceId', '$processActionName', $recordCount)
		""".stripMargin

		sqlServer.executeUpdate(query)

	}
}
