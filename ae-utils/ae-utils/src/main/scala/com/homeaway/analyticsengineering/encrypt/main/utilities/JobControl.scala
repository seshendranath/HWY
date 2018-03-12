package com.homeaway.analyticsengineering.encrypt.main.utilities

/**
  * Created by aguyyala on 11/09/17.
  */


import org.apache.spark.sql.SparkSession
import scala.collection.mutable

final class JobControl(spark: SparkSession) extends Utility {

	private val mysql = new MySql(spark)

	private val db = "ae_metastore"
	private val jobTable = "Job"
	private val JobInstanceTable = "JobInstance"
	private val JobInstanceLogTable = "JobInstanceLog"
	private val JobDependencyTable = "JobDependency"


	/**
	  * Checks whether the Job is Running or Not
	  *
	  * @param jobId : JobId
	  * @return true if Running else false
	  */
	def isRunning(jobId: Int): Boolean = {
		var isRunning = true
		val query =
			s"""
			   |SELECT COUNT(*) AS isRunning
			   |FROM $db.$JobInstanceTable
			   |WHERE jobId = $jobId AND statusFlag = 0
            """.stripMargin

		val rs = mysql.executeQuery(query)

		while (rs.next()) {
			isRunning = if (rs.getInt("isRunning") > 0) true else false
		}

		isRunning
	}


	/**
	  * Fetches the Active Flag of the Job
	  *
	  * @param jobId : Job Id
	  * @return Active Flag (true or false)
	  */
	def getActiveFlag(jobId: Int): Boolean = {

		var activeFlag = true

		val query =
			s"""
			   |SELECT activeFlag
			   |FROM $db.$jobTable
			   |WHERE JobId = $jobId
            """.stripMargin

		val rs = mysql.executeQuery(query)

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
			   |SELECT        JobId
			   |             ,lastSuccessfulJobStartTime
			   |             ,lastSuccessfulWaterMarkStartTime
			   |             ,lastSuccessfulWaterMarkStartDate
			   |             ,lastSuccessfulWaterMarkEndTime
			   |             ,lastSuccessfulWaterMarkEndDate
			   |             ,lastSuccessfulWaterMarkEndString
			   |             ,lastSuccessfulWaterMarkStartString
			   |FROM (
			   |SELECT        JobId
			   |             ,InstanceStartTime AS lastSuccessfulJobStartTime
			   |             ,LowWatermarkTime AS lastSuccessfulWaterMarkStartTime
			   |             ,CAST(LowWatermarkTime AS DATE) AS lastSuccessfulWaterMarkStartDate
			   |             ,HighWatermarkTime AS  lastSuccessfulWaterMarkEndTime
			   |             ,CAST(HighWatermarkTime AS DATE) AS  lastSuccessfulWaterMarkEndDate
			   |             ,HighWatermarkString AS  lastSuccessfulWaterMarkEndString
			   |             ,LowWatermarkString AS  lastSuccessfulWaterMarkStartString
			   |FROM $db.$JobInstanceTable
			   |WHERE JobId = $jobId AND statusFlag = 1
			   |ORDER BY HighWatermarkTime DESC,HighWatermarkString DESC LIMIT 1) a
            """.stripMargin

		val rs = mysql.executeQuery(query)

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
	  * get current job data process ranges based on dependencies Successful job's instance details
	  *
	  * @param jobId : Job Id's instance to be fetched
	  * @return current process start & end range details of a job in a Map
	  */
	def getCurrentRunDetails(jobId: Int): Map[String, String] = {

		val query =
			s"""
			   |SELECT
			   |     JobId,
			   |     currentprocesslwmtime,
			   |     coalesce(currentprocesshwmtime,currentprocesslwmtime) currentprocesshwmtime,
			   |     currentprocesslwmstring,
			   |     coalesce(currentprocesshwmstring,currentprocesslwmstring) currentprocesshwmstring
			   |     FROM
			   |     (SELECT
			   |     ji.jobid JobId,
			   |     max(ji.HighWatermarkTime) as currentprocesslwmtime,
			   |     min(Case When jdji.HighWatermarkTime > ji.HighWatermarkTime THEN jdji.HighWatermarkTime END) as currentprocesshwmtime,
			   |     max(ji.HighWatermarkString) as currentprocesslwmstring ,
			   |     min(CASE WHEN jdji.HighWatermarkString > ji.HighWatermarkString THEN jdji.HighWatermarkString END) as currentprocesshwmstring
			   |     From
			   |     (select jobid,max(highwatermarkstring) highwatermarkstring, max(highwatermarktime) highwatermarktime from $db.$JobInstanceTable where StatusFlag=1 group by jobid) ji
			   |     LEFT JOIN $db.$JobDependencyTable jd
			   |     ON ji.jobid=jd.jobid
			   |     And jd.activeflag=1
			   |     LEFT JOIN (select jobid,max(highwatermarkstring) highwatermarkstring, max(highwatermarktime) highwatermarktime from $db.$JobInstanceTable where StatusFlag=1 group by jobid) jdji
			   |     ON jdji.jobid=jd.Dependencyjobid
			   |     LEFT JOIN $db.$jobTable jdj
			   |     ON jdj.jobid = jd.jobid
			   |     And jdj.activeflag=1
			   |     Where ji.jobid=$jobId
			   |     Group by ji.jobid
			   |     ) currentrun
            """.stripMargin

		val rs = mysql.executeQuery(query)

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
			   |INSERT INTO $db.$JobInstanceTable(jobId, instanceId, InstanceStartTime, StatusFlag)
			   |VALUES ($jobId, '$instanceId', CURRENT_TIMESTAMP, 0)
			""".stripMargin

		mysql.executeUpdate(query)

		(instanceId, lsr)

	}


	/**
	  * End Job's Instance
	  *
	  * @param instanceId        : Instance Id to END
	  * @param statusFlag        : statusFlag 1 - SUCCEEDED, 0 - Running, -1 - FAILED
	  * @param LowWatermarkTime  : ETL Start Time
	  * @param HighWatermarkTime : ETL End Time
	  * @return 1
	  */
	def endJob(instanceId: String, statusFlag: Int, LowWatermarkTime: String = "1900-01-01",
	           HighWatermarkTime: String = "1900-01-01", LowWatermarkString: String = "N/A", HighWatermarkString: String = "N/A", EventStartDate: String = "1900-01-01", EventEndDate: String = "1900-01-01", EventDateInterval: String = "N/A"): Int = {

		val query =
			s"""
			   |UPDATE $db.$JobInstanceTable
			   |SET  statusFlag = $statusFlag
			   |    ,LowWatermarkTime = '$LowWatermarkTime'
			   |    ,HighWatermarkTime = '$HighWatermarkTime'
			   |    ,LowWatermarkString = '$LowWatermarkString'
			   |    ,HighWatermarkString = '$HighWatermarkString'
			   |    ,EventStartDate = '$EventStartDate'
			   |    ,EventEndDate = '$EventEndDate'
			   |    ,EventDateInterval = '$EventDateInterval'
			   |    ,InstanceEndTime = CURRENT_TIMESTAMP
			   |    ,UpdateDateTime = CURRENT_TIMESTAMP
			   |WHERE InstanceId = '$instanceId'
			""".stripMargin

		mysql.executeUpdate(query)

	}


	/**
	  * Inserts Job to Job Table
	  *
	  * @param objectName  : Object Name
	  * @param processName : Process Name
	  * @param jobName     : Job Name
	  */
	def insertJob(objectName: String, processName: String, jobName: String): Int = {

		val query =
			s"""
			   |INSERT INTO  $db.$jobTable(JobName, ProcessName, ObjectName, JobType, JobGroup, JobFrequency, Description, ActiveFlag)
			   |VALUES('$jobName', '$processName', '$objectName', '${spark.conf.get("jobType")}', '${spark.conf.get("jobGroup")}','${spark.conf.get("jobFrequency")}' , '${spark.conf.get("description")}', 1)
            """.stripMargin

		mysql.executeUpdate(query)
	}


	/**
	  * Fetches Job ID from Job Table
	  *
	  * @param objectName  : Object Name
	  * @param processName : Process Name
	  * @param jobName     : Job Name
	  * @return Job Id (return -1 if Job doesn't exist)
	  */
	def getJobId(jobName: String, objectName: String, processName: String): Int = {

		var jobId = -1

		val query =
			s"""
			   |SELECT JobId
			   |FROM $db.$jobTable
			   |WHERE objectName = '$objectName' AND processName = '$processName'
            """.stripMargin

		val rs = mysql.executeQuery(query)

		while (rs.next()) {
			jobId = rs.getInt("JobId")
		}

		if (jobId == -1) {
			log.warn("Job NOT FOUND in the Job Table, Adding it now.")
			insertJob(objectName: String, processName: String, jobName: String)
			jobId = getJobId(jobName, objectName, processName)

		}

		jobId
	}


	/**
	  * Logs Info to the JobInstanceLog Table
	  *
	  * @param instanceId        : Instance Id
	  * @param processActionName : Message of the Action
	  * @param recordCount       : Record Count
	  * @return 1
	  */
	def logInfo(instanceId: String, processActionName: String, recordCount: Long): Int = {

		val query =
			s"""
			   |INSERT INTO $db.$JobInstanceLogTable (InstanceId, InstanceStepName, RecordCount)
			   |VALUES ('$instanceId', '$processActionName', $recordCount)
            """.stripMargin

		mysql.executeUpdate(query)

	}
}
