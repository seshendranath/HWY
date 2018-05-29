package com.homeaway.analyticsengineering.task


/**
  * Created by aguyyala on 04/01/18.
  */


import java.util.UUID

import com.homeaway.analyticsengineering.AnalyticsTaskApp.conf
import com.homeaway.analyticsengineering.utilities.{Logging, SqlServer}
import com.homeaway.datatech.forgetme.model.enums
import com.homeaway.analyticsengineering.model._
import javax.sql.rowset.CachedRowSet

import scala.collection.mutable


object ForgetMeSqlOperations extends Logging {

	val ss = new SqlServer
	val forgetMeQueueTbl: String = conf.forgetMeQueueTbl
	val PIIMetadataTbl: String = conf.PIIMetadataTbl
	val obfuscatedKeyword: String = conf.obfuscatedKeyword


	/**
	  * Log each request to the queue table
	  */
	def logRequestToQueue(requestUuid: UUID, requestType: String, status: String): Unit = {
		val rs = ss.executeQuery(s"SELECT COUNT(*) AS cnt FROM $forgetMeQueueTbl WHERE RequestUUID = '$requestUuid'")
		rs.next()
		val isPresent = rs.getInt("cnt")

		if (isPresent == 0) {
			ss.executeUpdate(
				s"""
				   |INSERT INTO $forgetMeQueueTbl (RequestUUID, RequestType, status)
				   |VALUES ('$requestUuid', '$requestType', '$status')
				 """.stripMargin)
		}
		else {
			ss.executeUpdate(
				s"""
				   |UPDATE $forgetMeQueueTbl
				   |SET  RequestType = '$requestType'
				   |    ,status = '$status'
				   |    ,DWUpdateDate = GETDATE()
				   |WHERE RequestUUID = '$requestUuid'
				 """.stripMargin)
		}
	}


	/**
	  * Fetch all pending forgetme requests
	  */
	def getPendingForgetMeRequests: Array[RequestsToProcess] = {

		val query = if (conf.reprocess) {
			s"""
			   |SELECT
			   |     RequestType
			   |    ,RequestUUID
			   |    ,DWCreateDate
			   |FROM $forgetMeQueueTbl
			   |WHERE DWCreateDate >= DATEADD(day, -${conf.reprocessDays}, GETDATE())
			 """.stripMargin
		} else {
			s"""
			   |SELECT
			   |     RequestType
			   |    ,RequestUUID
			   |FROM $forgetMeQueueTbl
			   |WHERE
			   |     Status = '${enums.ResponsesType.INPROGRESS.getValue}'
			   |ORDER BY RequestType
			""".stripMargin

		}

		log.info(s"Running Query: $query")
		val rs = ss.executeQuery(query)

		var requestsToProcess = mutable.ArrayBuffer[RequestsToProcess]()
		while (rs.next()) {
			requestsToProcess += RequestsToProcess(rs.getString(1), UUID.fromString(rs.getString(2)))
		}

		requestsToProcess.toArray
	}


	def mashedCol(alias: String, c: String) = s"LOWER(CAST(ISNULL(CASE WHEN LTRIM(RTRIM($alias.$c)) = '' THEN 'null' ELSE LTRIM(RTRIM($alias.$c)) END, 'null') AS VARCHAR(2000)))"


	/**
	  * Auto generate Select Statement from Update Statement
	  */
	def getSelectStmtFromUpdate(updateStmt: String): (Array[String], String, String) = {

		log.info(s"Converting Update Query into Select Query")
		log.debug(s"Update Query Before Massaged: $updateStmt")

		val massagedUpdateStmt = updateStmt.toLowerCase.trim.split('\n').map(_.trim.filter(_ >= ' ')).mkString(" ")
		log.debug(s"Update Query After Massaged: $massagedUpdateStmt")

		val pattern = conf.updatePattern

		log.debug("parse through update pattern")
		val pattern(_, alias, _, updatedCols, rest) = massagedUpdateStmt

		val substitutePattern = conf.substitutePattern

		log.debug("parse through substitute pattern")
		val cols = substitutePattern.findAllIn(updatedCols).matchData.map(_.group(1)).filter(_.endsWith(obfuscatedKeyword)).map(_.replace(obfuscatedKeyword, "")).map(_.trim).toArray

		val castedCols = cols.map(c => s"${mashedCol(alias, c)} AS $c").mkString(", ")

		val addWhereClause = if (cols.nonEmpty) " AND " + cols.map(c => s"${mashedCol(alias, c)} = '#{$c}'").mkString(" AND ") else ""

		val selectStmt = "SELECT DISTINCT " + castedCols + rest

		log.debug(s"Affected Columns: ${cols.mkString(", ")}")
		log.debug(s"Select Query: $selectStmt")

		(cols, selectStmt, massagedUpdateStmt + addWhereClause)

	}


	/**
	  * Get list of all Active Tables
	  */
	def getActiveTables(all: Boolean = false): mutable.LinkedHashMap[String, TableProperties] = {
		val activeFlag = if (!all) "AND ActiveFlag = 1" else ""

		var query =
			s"""
			   |SELECT
			   |DISTINCT
			   |       LTRIM(RTRIM(LTRIM(RTRIM(DatabaseSchema)) + '.' + LTRIM(RTRIM(TableName)))) AS tbl
			   |      ,UpdateStatement
			   |      ,SearchKey
			   |      ,SortOrder
			   |FROM $PIIMetadataTbl
			   |WHERE UpdateStatement IS NOT NULL
			   |$activeFlag
			   |ORDER BY SortOrder
			 """.stripMargin

		log.info(s"Running Query: $query")
		val rs = ss.executeQuery(query)

		val activeTables = mutable.LinkedHashMap[String, TableProperties]()
		while (rs.next()) {

			val tbl = rs.getString(1)
			val updateStmt = rs.getString(2)
			val searchKey = rs.getString(3)
			val (cols, selectStmt, massagedUpdateStmt) = getSelectStmtFromUpdate(updateStmt)

			query =
				s"""
				   |SELECT
				   |     LOWER(ColumnName) AS ColumnName
				   |    ,MaskTypeClassification
				   |    ,ISNULL(ColumnLength, -1) AS lengthLimitOfObfuscatedValue
				   |FROM $PIIMetadataTbl
				   |WHERE ActiveFlag = 1 AND LTRIM(RTRIM(LTRIM(RTRIM(DatabaseSchema)) + '.' + LTRIM(RTRIM(TableName)))) = '$tbl'
				 """.stripMargin

			log.info(s"Running Query: $query")
			val colPIIRs = ss.executeQuery(query)

			val colMaskTypeMap = mutable.Map[String, ColumnProperties]()
			while (colPIIRs.next()) {
				colMaskTypeMap += colPIIRs.getString(1) -> ColumnProperties(enums.MaskType.getMaskTypeByValue(colPIIRs.getString(2)), colPIIRs.getInt(3))
			}


			activeTables += (tbl ->
				TableProperties(cols, selectStmt, massagedUpdateStmt, searchKey, colMaskTypeMap.toMap))
		}


		activeTables
	}


	/**
	  * Execute Select Statement and prepare a Map of column to values
	  */
	def executeSelectStmt(selectStmt: String, cols: Array[String]): Map[String, Array[String]] = {

		val colMap = mutable.Map[String, mutable.ArrayBuffer[String]]()

		log.debug(s"Running Query: $selectStmt")
		val rs = ss.executeQuery(selectStmt)

		log.debug(s"Parsing Select Stmt Results")
		while (rs.next()) {

			var i = 0
			for (col <- cols) {
				i += 1
				if (colMap contains col) {
					val ab = colMap(col)
					ab.append(rs.getString(i))
					colMap += (col -> ab)
				} else {
					colMap += (col -> mutable.ArrayBuffer[String](rs.getString(i)))
				}
			}

		}
		log.debug(s"Parsing Complete Select Stmt Results")
		colMap.mapValues(_.toArray).toMap
	}


	/**
	  * Update Request Status in the Queue Table
	  */
	def updateRequestStatus(requestType: String, requestUuid: UUID, status: String, emailSha256Hash: String, obfuscatedEmailAddress: String): Int = {
		ss.executeUpdate(
			s"""
			   |UPDATE $forgetMeQueueTbl
			   |SET  RequestType = '$requestType'
			   |    ,status = '$status'
			   |    ,EmailSha256Hash = '$emailSha256Hash'
			   |    ,ObfuscatedEmailAddress = '$obfuscatedEmailAddress'
			   |    ,DWUpdateDate = GETDATE()
			   |WHERE RequestUUID = '$requestUuid'
			 """.stripMargin)
	}


	/**
	  * Generate Insert Statement to log the Request into Log Table
	  */
	def getInsertToLogTbl(requestUuid: UUID, tbl: String, count: Int, SelectStatement: String = null): String = {
		val query =
			s"""
			   |INSERT INTO ${conf.obfuscationLogTbl} (RequestUUID, DatabaseSchema, TableName, RecordCount, SelectStatement)
			   |VALUES ('$requestUuid', '${tbl.split("\\.").take(2).mkString(".")}', '${tbl.split("\\.").last}', $count, '$SelectStatement')
			 """.stripMargin
		query
	}


	/**
	  * Check if the Request is already processed
	  */
	def checkIfAlreadyProcessed(requestUuid: UUID, tbl: String): Int = {
		val whereClause = if (!conf.softCheck) "AND RecordCount <> 0" else ""

		val query =
			s"""
			   |SELECT COUNT(*) AS CNT FROM ${conf.obfuscationLogTbl}
			   |WHERE RequestUUID = '$requestUuid'
			   | AND DatabaseSchema = '${tbl.split("\\.").take(2).mkString(".")}'
			   | AND TableName = '${tbl.split("\\.").last}'
			   | $whereClause
			 """.stripMargin
		log.info(s"Checking if the request $requestUuid for $tbl is already processed $query")
		ForgetMeSqlOperations.getCountFromSelect(query)
	}


	/**
	  * Check if the Requests are already processed
	  */
	def checkIfAlreadyProcessedAtOnce(requestUuids: Seq[UUID], tbl: String): Int = {
		val whereClause = if (!conf.softCheck) "AND RecordCount <> 0" else ""

		val query =
			s"""
			   |SELECT COUNT(DISTINCT RequestUUID) AS CNT FROM ${conf.obfuscationLogTbl}
			   |WHERE RequestUUID IN (${requestUuids.mkString("'", "','", "'")})
			   | AND DatabaseSchema = '${tbl.split("\\.").take(2).mkString(".")}'
			   | AND TableName = '${tbl.split("\\.").last}'
			   | $whereClause
			 """.stripMargin
		log.info(s"Checking if the request all requests for $tbl is already processed $query")
		ForgetMeSqlOperations.getCountFromSelect(query)
	}


	/**
	  * Create a Map of Request Count for give a select statement
	  */
	def getRequestCnt(query: String): Map[String, Int] = {
		log.debug(s"Running Query: $query")
		val rs = ss.executeQuery(query)

		val requestCntMap = mutable.Map[String, Int]()
		log.debug(s"Parsing Select Stmt Results to find out Request Count in the table")
		while (rs.next()) {
			requestCntMap += rs.getString(1) -> rs.getInt(2)
		}
		log.debug(s"Parsing Complete Select Stmt Results to find out Request Count in the table")

		requestCntMap.toMap
	}


	/**
	  * Get the existing status of request in the queue table
	  */
	def getRequestPreviousStatus(requestUuid: UUID, status: String): Int = {
		val query =
			s"""
			  |SELECT COUNT(*) AS cnt FROM
			  |DW_Config.dbo.GDPRQueue
			  |WHERE RequestUUID = '$requestUuid' AND STATUS = '$status'
			""".stripMargin

		getCountFromSelect(query)
	}


	/**
	  * Execute update statement
	  */
	def executeUpdateStmt(query: String): Int = {
		log.debug(s"Running Query: $query")
		ss.executeUpdate(query)
	}


	/**
	  * Execute select statement
	  */
	def executeSelectStmt(query: String): CachedRowSet = {
		log.debug(s"Running Query: $query")
		ss.executeQuery(query)
	}


	/**
	  * Get count from a select statement
	  */
	def getCountFromSelect(query: String): Int = {
		log.debug(s"Running Query: $query")
		val rs = ss.executeQuery(query)
		rs.next()
		rs.getInt(1)
	}
}
