package com.homeaway.analyticsengineering.task

/**
  * Created by aguyyala on 04/01/18.
  */


import java.util.UUID

import com.homeaway.analyticsengineering.AnalyticsTaskApp.conf
import com.homeaway.analyticsengineering.enums.SearchKeys
import com.homeaway.analyticsengineering.model._
import com.homeaway.analyticsengineering.utilities.{Logging, Utility}
import com.homeaway.datatech.forgetme.model.enums.{RequestType, ResponsesType}
import com.homeaway.datatech.forgetme.model.{ObfuscateMapping, ObfuscateMappingValues, enums}
import com.microsoft.sqlserver.jdbc.SQLServerException

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._


class ForgetMeRequestProcessorAtOnce extends Logging {

	val registrationUuid: UUID = conf.registrationUuid // ForgetMe.getRegistrationUuid
	log.info("Registration UUID: " + registrationUuid)


	def run(): Unit = {
		log.info(s"Get All ForgetMe requests to Process from Queue Table")
		val requestsToProcess: Array[RequestsToProcess] = ForgetMeSqlOperations.getPendingForgetMeRequests
		log.info(s"ForgetMe Requests to process: ${requestsToProcess.mkString(", ")}")
		if (requestsToProcess.isEmpty) {
			log.info("NO REQUESTS TO PROCESS")
			return
		}


		log.info("Get All Active Tables to Scan for PII Data")
		val activeTables: mutable.LinkedHashMap[String, TableProperties] = ForgetMeSqlOperations.getActiveTables()

		log.debug(s"Active Tables Map: $activeTables")

		log.info("Start Processing ForgetMe Requests")
		val (requestsMap, obfuscationMap) = constructRequestsMap(requestsToProcess)
		log.debug(s"Requests Map ${requestsToProcess.length} and ${requestsMap.size}: $requestsMap")

		// Process Requests
		processRequests(requestsMap, activeTables, requestsToProcess, obfuscationMap)

		log.info("ALL REQUESTS PROCESSED")
	}


	/**
	  * Send Status to Kafka and Update the status of each processed request in Queue Table
	  */
	def sendStatusToKafka(requestType: String, requestsToProcess: Array[RequestsToProcess], logRequestsMap: mutable.Map[String, ListBuffer[LogRequests]], requestsMap: Map[UUID, RequestsMap], activeTables: mutable.LinkedHashMap[String, TableProperties]): Unit = {
		val kafka = new Kafka
		requestsToProcess.filter(x => x.requestType == requestType && !(logRequestsMap.keys.toSet contains x.requestUuid.toString)).foreach { req =>
			val requestType = req.requestType
			val requestUuid = req.requestUuid
			val emptyRequestMap = RequestsMap("", "", "", "", "", "", "")
			updateStatus(requestType, requestUuid, ResponsesType.NOTFOUND, requestsMap.getOrElse(requestUuid, emptyRequestMap).sha256Email, requestsMap.getOrElse(requestUuid, emptyRequestMap).obfuscatedEmail)
		}

		logRequestsMap.filterKeys(k => requestsMap(UUID.fromString(k)).requestType == requestType).map { case (requestUUID, logRequests) => (requestUUID, logRequests.map(_.cnt).sum, logRequests) }.foreach {
			case (requestUUID, totalCount, _) =>

				val requestUuid = UUID.fromString(requestUUID)

				val req = requestsMap(requestUuid)
				val (requestType, sha256Email, obfuscatedEmail) = (req.requestType, req.sha256Email, req.obfuscatedEmail)

				if (totalCount > 0) updateStatus(requestType, requestUuid, ResponsesType.COMPLETED, sha256Email, obfuscatedEmail)
				else updateStatus(requestType, requestUuid, ResponsesType.NOTFOUND, sha256Email, obfuscatedEmail)
		}

		// Helper function to update status
		def updateStatus(requestType: String, requestUuid: UUID, status: ResponsesType, sha256Email: String = "", obfuscatedEmail: String = ""): Unit = {

			val updatedStatus = if (ForgetMeAPI.getRequestStatusByValues(requestType, requestUuid) == 1) ResponsesType.COMPLETED else status

			log.info(s"Updating Request Status for $requestUuid to $updatedStatus")
			ForgetMeSqlOperations.updateRequestStatus(requestType, requestUuid, updatedStatus.getValue, sha256Email, obfuscatedEmail)
			log.info(s"Sending Request Status for $requestUuid as $updatedStatus to Kafka")
			kafka.sendAck(registrationUuid, requestUuid, updatedStatus, updatedStatus.getValue)
		}
	}


	// Construct Requests Map with all the details of a request
	def constructRequestsMap(requestsToProcess: Array[RequestsToProcess]): (Map[UUID, RequestsMap], Map[String, mutable.Set[String]]) = {

		val requestMap = mutable.Map[UUID, RequestsMap]()
		val obfuscationMap = mutable.Map[String, mutable.Set[String]]()

		requestsToProcess.foreach { req =>

			log.debug(s"Processing Request $req")

			val requestType = req.requestType
			val requestUuid = req.requestUuid

			try {

				val request = ForgetMeAPI.getForgetMeRequest(requestUuid)

				log.debug("Fetching Email")
				val email = request.getCustomerDetails.getEmail.toLowerCase.replaceAll("'", "''")

				log.debug(s"Getting Obfusacted Email for ForgetMeNot request")
				val (_, sha256Email, obfuscatedEmail) = ForgetMeAPI.getEmailDetails(registrationUuid, requestType, requestUuid)

				log.debug("Fetching User ID")
				val userID = checkNulls(request.getCustomerDetails.getUserID)

				log.debug("Fetching User UUID")
				val userUUID = checkNulls(request.getCustomerDetails.getUserUUID)

				log.debug("Fetching Account UUID")
				val userAccounts = request.getCustomerDetails.getUserAccounts
				val accountUUID = if (userAccounts == null) "" else checkNulls(userAccounts.asScala.map(_.getAccountUUID).mkString(","))

				val searchKeySpace: RequestsMap = RequestsMap(requestType, email, obfuscatedEmail, sha256Email, userID, userUUID, accountUUID)

				requestMap += requestUuid -> searchKeySpace
				obfuscationMap += email.toLowerCase -> (obfuscationMap.getOrElse(email.toLowerCase, mutable.Set[String]()) += obfuscatedEmail)
			} catch {
				case e: RuntimeException => e.printStackTrace()
					log.error(s"ERROR MESSAGE: ${e.getMessage}")
					log.error(s"Got Error while processing $requestType Request: $requestUuid")
			}
		}
		(requestMap.toMap, obfuscationMap.toMap)
	}


	// Process Request
	def processRequests(requestsMap: Map[UUID, RequestsMap], activeTables: mutable.LinkedHashMap[String, TableProperties], requestsToProcess: Array[RequestsToProcess]
	                    , obfuscationMap: Map[String, mutable.Set[String]]): Map[String, ListBuffer[LogRequests]] = {

		val forgetMeRequests = requestsMap.filter(_._2.requestType == RequestType.ForgetMe.getValue)
		val forgetMeNotRequests = requestsMap.filter(_._2.requestType == RequestType.ForgetMeNot.getValue)

		val emailsToObfuscate = forgetMeRequests.mapValues(_.email).values.map(_.trim).mkString("'", "','", "'")
		val obfuscatedemailsToObfuscate = forgetMeRequests.mapValues(_.obfuscatedEmail).values.map(_.trim).mkString("'", "','", "'")
		val userIDsToObfuscate = forgetMeRequests.mapValues(_.userID).values.map(_.trim).filterNot(_ == "").mkString(",")
		val userUUIDsToObfuscate = forgetMeRequests.mapValues(_.userUUID).values.map(_.trim).filterNot(_ == "").mkString("'", "','", "'")
		val accountUUIDsToObfuscate = forgetMeRequests.flatMap(_._2.accountUUID.split(",")).map(_.trim).filterNot(_ == "").mkString("'", "','", "'")

		val forgetMeSearchKeySpace = Map(
			SearchKeys.EMAIL.toString -> emailsToObfuscate
			, ForgetMeAPI.addObfuscateKeyword(SearchKeys.EMAIL.toString) -> obfuscatedemailsToObfuscate
			, SearchKeys.USERID.toString -> userIDsToObfuscate
			, SearchKeys.USERUUID.toString -> userUUIDsToObfuscate
			, SearchKeys.ACCOUNTUUID.toString -> accountUUIDsToObfuscate
		)

		log.debug(s"Emails to be Obfuscated: $emailsToObfuscate")
		log.info(s"UserIDs to be Obfuscated: $userIDsToObfuscate")
		log.info(s"UserUUIDs to be Obfuscated: $userUUIDsToObfuscate")
		log.info(s"AccountUUIDs to be Obfuscated: $accountUUIDsToObfuscate")


		val emailsToDeObfuscate = forgetMeNotRequests.mapValues(_.obfuscatedEmail).values.map(_.trim).mkString("'", "','", "'")
		val userIDsToDeObfuscate = forgetMeNotRequests.mapValues(_.userID).values.map(_.trim).filterNot(_ == "").mkString(",")
		val userUUIDsToDeObfuscate = forgetMeNotRequests.mapValues(_.userUUID).values.map(_.trim).filterNot(_ == "").mkString("'", "','", "'")
		val accountUUIDsToDeObfuscate = forgetMeNotRequests.flatMap(_._2.accountUUID.split(",")).map(_.trim).filterNot(_ == "").mkString("'", "','", "'")

		val forgetMeNotSearchKeySpace = Map(
			SearchKeys.EMAIL.toString -> emailsToDeObfuscate
			, ForgetMeAPI.addObfuscateKeyword(SearchKeys.EMAIL.toString) -> emailsToDeObfuscate
			, SearchKeys.USERID.toString -> userIDsToDeObfuscate
			, SearchKeys.USERUUID.toString -> userUUIDsToDeObfuscate
			, SearchKeys.ACCOUNTUUID.toString -> accountUUIDsToDeObfuscate
		)

		log.debug(s"Emails to be DeObfuscated: $emailsToDeObfuscate")
		log.info(s"UserIDs to be DeObfuscated: $userIDsToDeObfuscate")
		log.info(s"UserUUIDs to be DeObfuscated: $userUUIDsToDeObfuscate")
		log.info(s"AccountUUIDs to be DeObfuscated: $accountUUIDsToDeObfuscate")

		val requestsObfuscateMapping = mutable.Map[String, mutable.Set[ObfuscateMappingValues]]()
		val obfuscateMappingList = ListBuffer[(String, ObfuscateMapping)]()
		val logRequestsMap = mutable.Map[String, ListBuffer[LogRequests]]()

		// Process All forgetMe Requests
		if (forgetMeRequests.nonEmpty) {
			for ((tbl, props) <- activeTables) {
				breakable {
					log.info(s"Processing Table: $tbl")

					// Generate a selecte statement to be logged into Log Table
					val logSelectStmt = constructSelectStmt(props.selectStmt, forgetMeRequests, ForgetMeAPI.addObfuscateKeyword(props.searchKey), forgetMeSearchKeySpace)

					// Check if the requests are already processed for a table
					if (conf.softCheck) {
						val requestsCnt = ForgetMeSqlOperations.checkIfAlreadyProcessedAtOnce(forgetMeRequests.keys.toList, tbl)
						if (requestsCnt == forgetMeRequests.keys.size) {
							log.info(s"All Requests ALREADY Processed/Obfuscated/DeObfuscated for Table $tbl")
							logRequests(logRequestsMap, forgetMeRequests.keys.map(x => (x.toString, 0)).toMap, tbl, logSelectStmt)
							break
						}
					}

					// Check if the table doesn't have any columns to obfuscate
					if (props.cols.isEmpty) {
						log.info(s"Plain Update Statement with no columns to obfuscate")

						val plainUpdatePattern = "(.*)where\\s+([\\w\\.]+).*".r
						val plainUpdatePattern(updateStmt, _) = props.updateStmt

						val conf.selectPattern(_, _, from, _, replaceKey, _) = props.selectStmt
						selectAndLog(requestsMap, RequestType.ForgetMe.getValue, replaceKey, props.searchKey, forgetMeSearchKeySpace, logSelectStmt, from, tbl, logRequestsMap)
						val s = System.nanoTime()
						ForgetMeSqlOperations.executeUpdateStmt(s"$updateStmt WHERE $replaceKey IN ( ${forgetMeSearchKeySpace(props.searchKey)} )")
						Utility.timeit(s, s"Run Time of Plain UPDATE for $tbl")
						break
					}

					// Construct the select statement to be executed and fetch results
					val selectStmt = constructSelectStmt(props.selectStmt, forgetMeRequests, props.searchKey, forgetMeSearchKeySpace)

					log.debug(s"Updated Select Stmt: $selectStmt")

					// Execute the constructed select statement and get results to be obfuscated
					val s = System.nanoTime()
					val results: Map[String, Array[String]] = ForgetMeSqlOperations.executeSelectStmt(selectStmt, Array(conf.requestUUIDCol) ++ props.cols)
					Utility.timeit(s, s"Run Time of SELECT for $tbl")
					log.debug(s"Got Results from Select Stmt for $tbl $results")

					// Check if the results are empty
					if (results.isEmpty) {
						log.info(s"No Results FOUND for Table $tbl")
						logRequests(logRequestsMap, forgetMeRequests.keys.map(x => (x.toString, 0)).toMap, tbl, logSelectStmt)
						break
					}

					// Check request count in the results and log them
					val requestsCnt = results(conf.requestUUIDCol).groupBy(identity).mapValues(_.length)
					logRequests(logRequestsMap, requestsCnt, tbl, logSelectStmt)

					// If there are results, add them to ObfucateMappingValues to obfuscate
					log.info(s"Results FOUND for Table $tbl")
					val addMapping = ForgetMeAPI.processResultsAndGetObArrayAtOnce(tbl, results, props, requestsMap)
					addMapping.foreach { case (requestUuid, v) =>
						requestsObfuscateMapping += requestUuid -> (requestsObfuscateMapping.getOrElse(requestUuid, mutable.Set[ObfuscateMappingValues]()) ++= v)
					}
				}
			}

			// Check of ObfuscateMapping requests seq is empty
			if (requestsObfuscateMapping.nonEmpty) {
				log.info(s"Fetch Obfuscate Mapping for all ForgetMe Requests")
				requestsObfuscateMapping.foreach { case (requestUuid, obSet) =>
					log.info(s"Constructing Obfuscation request for $requestUuid")
					obSet.foreach(log.debug)

					val completeObArray = ForgetMeAPI.getCompleteObArray(registrationUuid, RequestType.ForgetMe.getValue, UUID.fromString(requestUuid), obSet.toList)

					// Make ForgetMe API Call and get obfuscated results back
					val obfuscateMapping = ForgetMeAPI.fetchObfuscatedresults(RequestType.ForgetMe.getValue, registrationUuid, UUID.fromString(requestUuid), completeObArray)
					log.debug(s"ObfuscateMapping Results:")
					ForgetMeAPI.printJSONObject(obfuscateMapping.getObfuscateMappingValues.asScala.toList)

					obfuscateMappingList += ((RequestType.ForgetMe.getValue, obfuscateMapping))
				}
			}
		}


		// Process ForgetMe Not Requests
		if (forgetMeNotRequests.nonEmpty) {
			log.info(s"Fetch Obfuscate Mapping for all ForgetMeNot Requests")
			forgetMeNotRequests.map { case (requestUUID, _) =>
				obfuscateMappingList += ((RequestType.ForgetMeNot.getValue, ForgetMeAPI.fetchObfuscatedresults(RequestType.ForgetMeNot.getValue, registrationUuid, requestUUID)))
			}
		}

		// Create Table Column Values Map
		val tblColValuesMap = prepareTableColumnValuesMap(obfuscateMappingList.toSet, activeTables)

		log.info("Process all ForgetMe Requests for each table")
		activeTables.keys.toArray.foreach { tbl =>
			if (tblColValuesMap contains tbl) {
				if (forgetMeRequests.nonEmpty) {
					log.debug(s"Executing Update for $tbl ForgetMe Requests")
					executeUpdate(RequestType.ForgetMe.getValue, forgetMeSearchKeySpace, forgetMeRequests, tbl)
				}
			}

			// Log every request for the table
			logTableRequests(forgetMeRequests, logRequestsMap, tbl, activeTables, forgetMeSearchKeySpace)
		}

		log.info("Sending ForgetMe Status to Kafka")
		sendStatusToKafka(RequestType.ForgetMe.getValue, requestsToProcess, logRequestsMap, requestsMap, activeTables)

		log.info("Process all ForgetMeNot Requests for each table")
		activeTables.keys.toArray.foreach { tbl =>
			if (tblColValuesMap contains tbl) {
				if (forgetMeNotRequests.nonEmpty) {
					log.debug(s"Executing Update for $tbl ForgetMeNot Requests")
					executeUpdate(RequestType.ForgetMeNot.getValue, forgetMeNotSearchKeySpace, forgetMeNotRequests, tbl)
				}
			}

			// Log every request for the table
			logTableRequests(forgetMeNotRequests, logRequestsMap, tbl, activeTables, forgetMeNotSearchKeySpace)
		}

		log.info("Sending ForgetMeNot Status to Kafka")
		sendStatusToKafka(RequestType.ForgetMeNot.getValue, requestsToProcess, logRequestsMap, requestsMap, activeTables)


		// Construct and execute update statement
		def executeUpdate(requestType: String, searchKeySpace: Map[String, String], requestsMap: Map[UUID, RequestsMap], tbl: String): Unit = {

			log.debug("Executing Update module")
			val colValuesMap = tblColValuesMap(tbl)
			val props = activeTables(tbl)
			val conf.selectPattern(_, _, from, _, replaceKey, searchKey) = props.selectStmt

			val conf.updatePattern(_, alias, _, updatedCols, _) = props.updateStmt

			log.debug("Constructing unobfuscatedCols cols")
			var unobfuscatedCols = updatedCols.split(",").filterNot(_ contains conf.obfuscatedKeyword).mkString(",\n")

			if (unobfuscatedCols != "") unobfuscatedCols += ","

			log.debug("Constructing Update Case Clause")
			val updateCaseClause = colValuesMap.map { case (col, values) =>
				if (!values.exists(_.requestType == requestType)) return
				values.filter(_.requestType == requestType).map { v =>
					s" WHEN $replaceKey = '${v.replaceKey}' AND ${ForgetMeSqlOperations.mashedCol(alias, col)} = '${v.originalValue.toLowerCase}' THEN '${v.replaceValue}' "
				}.mkString(s"$alias.$col = \nCASE\n", "\n", s"\nELSE $alias.$col END")
			}.mkString(",\n")

			log.debug("Constructing Update Stmt")
			val updateStmt = s"UPDATE $alias SET\n $unobfuscatedCols\n $updateCaseClause \n$from \nWHERE $replaceKey IN ( ${searchKeySpace(searchKey)} )"

			if (requestType == RequestType.ForgetMeNot.getValue) {
				log.debug("Constructing Log Stmt")
				val logSelectStmt = constructSelectStmt(props.selectStmt, requestsMap, ForgetMeAPI.addObfuscateKeyword(searchKey), searchKeySpace)

				log.debug("Constructing Request Cnt Map")
				val requestCntMap = selectAndLog(requestsMap, requestType, replaceKey, searchKey, searchKeySpace, logSelectStmt, from, tbl, logRequestsMap)

				if (requestCntMap.nonEmpty) performUpdate()
				else logRequests(logRequestsMap, requestsMap.keys.map(x => (x.toString, 0)).toMap, tbl, logSelectStmt)

			} else performUpdate()

			def performUpdate(): Unit = {
				log.debug(s"Update Stmt for $requestType: $updateStmt")
				var done = false
				var cnt = 300
				while (!done) {
					if (cnt <= 0) throw new Exception(s"Exceeded duplicate Threshold Limit for updating $tbl")
					try {
						val s = System.nanoTime()
						ForgetMeSqlOperations.executeUpdateStmt(updateStmt)
						Utility.timeit(s, s"Run Time of UPDATE for $tbl")
						done = true
					} catch {
						case e: SQLServerException => cnt -= 1; handleDuplicateKeyException(requestType, e, tbl, replaceKey, alias, from, updateStmt, obfuscationMap)
					}
				}
			}
		}

		logRequestsMap.toMap
	}


	/**
	  * Handle Duplicate Key Exception
	  */
	def handleDuplicateKeyException(requestType: String, e: Exception, tbl: String, replaceKey: String, alias: String, from: String, updateStmt: String
	                                , obfuscationMap: Map[String, mutable.Set[String]], cnt: Int = 1): Unit = {
		log.error(s"FOUND SQLServerException while processing $tbl")
		log.error(e.getMessage)

		if (cnt > 30) {
			log.error(s"Exceeding the threshold of recursion.")
			log.error(s"Might be entering into a infinite loop. Please check.")
			throw e
		}

		if (e.getMessage contains "Cannot insert duplicate key row") {

			val dupPattern = ".*The duplicate key value is \\((.*)\\).*".r
			val dupPattern(extractedDuplicateKey) = e.getMessage

			log.error(s"FOUND Duplicate Key for $tbl : $extractedDuplicateKey")

			val (duplicateKey, caseClause) = if (cnt == 1 && requestType == RequestType.ForgetMeNot.getValue) {
				val obValue = obfuscationMap(extractedDuplicateKey.toLowerCase).mkString("'", "','", "'")
				log.error(s"Duplicate Key For ForgetMeNot. Changing obfuscation value itself $obValue")
				(obValue, obfuscationMap(extractedDuplicateKey.toLowerCase).map{v => s" WHEN $replaceKey = '$v' THEN '${v + "_DUP"}'"}.mkString("CASE\n", "\n", s"ELSE $replaceKey END"))
			} else ("'" + extractedDuplicateKey + "'", s"CASE WHEN $replaceKey = '$extractedDuplicateKey' THEN '${extractedDuplicateKey + "_DUP"}' ELSE $replaceKey END")

			// Replace Duplicate Key by adding _DUP to the end of the key
			val updateStmtDUP = s"UPDATE $alias SET $replaceKey = $caseClause \n$from \nWHERE $replaceKey IN ($duplicateKey)"

			var done = false
			var whileCnt = 300
			while (!done) {
				if (whileCnt <= 0) throw new Exception(s"Exceeded duplicate Threshold Limit for updating $tbl in handle duplicate module")
				try {
					// Execute the update statement to replace duplicate key
					val s = System.nanoTime()
					ForgetMeSqlOperations.executeUpdateStmt(updateStmtDUP)
					Utility.timeit(s, s"Run Time of UPDATE Duplicates for $tbl")
					done = true
				} catch {
					case e: SQLServerException =>
						whileCnt -= 1
						// Recursively call handleDuplicateKeyException if the replaced duplicate key is already present
						handleDuplicateKeyException(requestType, e, tbl, replaceKey, alias, from, updateStmt, obfuscationMap, cnt + 1)

						val s = System.nanoTime()
						ForgetMeSqlOperations.executeUpdateStmt(updateStmtDUP)
						Utility.timeit(s, s"Run Time of UPDATE Duplicates for $tbl")
				}
			}

		} else throw e
	}


	// Prepare Table Column Values Map to obfuscate/deobfuscate
	def prepareTableColumnValuesMap(obfuscatedResults: Set[(String, ObfuscateMapping)], activeTables: mutable.LinkedHashMap[String, TableProperties]): Map[String, mutable.Map[String, mutable.Set[Values]]] = {

		log.info(s"Preparing Table Column Values Map")
		val tblColValuesMap = mutable.Map[String, mutable.Map[String, mutable.Set[Values]]]()

		obfuscatedResults.foreach { case (requestType, obfuscateMapping) =>
			obfuscateMapping.getObfuscateMappingValues.asScala.foreach { r =>

				breakable {
					if (r.getMetadata == null) break
					val maskType = r.getMaskType
					val metaCol = r.getMetadata.asScala.getOrElse("column", "").toString
					val metaTbl = r.getMetadata.asScala.getOrElse("table", "").toString
					val metaReplaceKey = r.getMetadata.asScala.getOrElse("replaceKey", "").toString
					val metaObfuscatedReplaceKey = r.getMetadata.asScala.getOrElse("obfuscatedReplaceKey", "").toString

					val valueToEncrypt = r.getValueToEncrypt.replaceAll("'", "''")
					val obfuscatedValue = r.getObfuscatedValue

					if (metaReplaceKey == "" || metaObfuscatedReplaceKey == "") break
					if ((activeTables contains metaTbl) && (maskType != activeTables(metaTbl).colMaskTypeMap.getOrElse(metaCol, ColumnProperties(enums.MaskType.STRING, 25)).maskType)) break

					val (replaceKey, originalValue, replaceValue) = if (requestType == RequestType.ForgetMe.getValue) {
						(metaReplaceKey, valueToEncrypt, obfuscatedValue)
					} else (metaObfuscatedReplaceKey, obfuscatedValue, valueToEncrypt)

					// Prepare a Map that consists of all the Tables and their results to be obfuscated/deobfuscated
					tblColValuesMap += metaTbl -> {
						val existingColValuesMap = tblColValuesMap.getOrElse(metaTbl, mutable.Map[String, mutable.Set[Values]]())
						existingColValuesMap += (metaCol -> (existingColValuesMap.getOrElse(metaCol, mutable.Set[Values]()) += Values(requestType, replaceKey, originalValue, replaceValue)))
					}
				}
			}
		}

		tblColValuesMap.toMap
	}


	// Generate case clause of all requests
	def getRequestUUIDCase(requestsMap: Map[UUID, RequestsMap], replaceKey: String, searchKey: String): String = {
		val caseClause = requestsMap.mapValues(ForgetMeAPI.extract(_, searchKey)).map { case (requestUUID, searchValue) =>
			s" WHEN $replaceKey = '$searchValue' THEN '$requestUUID'"
		}.mkString("CASE\n", "\n", s" END AS ${conf.requestUUIDCol}")
		caseClause
	}


	// Log count of each request and select statement to the log table
	def selectAndLog(requestsMap: Map[UUID, RequestsMap], requestType: String, replaceKey: String, searchKey: String, searchKeySpace: Map[String, String], logSelectStmt: String, from: String,
	                 tbl: String, logRequestsMap: mutable.Map[String, ListBuffer[LogRequests]]): Map[String, Int] = {
		val caseClause = getRequestUUIDCase(requestsMap, replaceKey, if (requestType == RequestType.ForgetMe.getValue) searchKey else ForgetMeAPI.addObfuscateKeyword(searchKey))
		val selectStmt = s"SELECT ${conf.requestUUIDCol}, COUNT(*) AS cnt FROM (SELECT $caseClause \n$from \nWHERE $replaceKey IN ( ${searchKeySpace(searchKey)} ) ) tmp GROUP BY ${conf.requestUUIDCol}"
		val s = System.nanoTime()
		val requestCntMap = ForgetMeSqlOperations.getRequestCnt(selectStmt)
		Utility.timeit(s, s"Run Time of SELECT for $tbl to fetch Request Count Map")

		logRequests(logRequestsMap, requestCntMap, tbl, logSelectStmt)
		requestCntMap
	}


	// Construct select statement
	def constructSelectStmt(selectStmt: String, requestsMap: Map[UUID, RequestsMap], searchKey: String, searchKeySpace: Map[String, String]): String = {
		val conf.selectPattern(select, cols, from, _, replaceKey, _) = selectStmt

		val caseClause = getRequestUUIDCase(requestsMap, replaceKey, searchKey)

		s"$select $caseClause, $cols $from WHERE $replaceKey IN ( ${searchKeySpace(searchKey)} )"
	}


	// Create a Seq of LogRequests which consists of Table, Count, and the selectStmt
	def logRequests(logRequestsMap: mutable.Map[String, ListBuffer[LogRequests]], requests: Map[String, Int], tbl: String, logSelectStmt: String): Unit = {
		requests.foreach { case (requestUUID, cnt) =>
			logRequestsMap += requestUUID.toString -> (logRequestsMap.getOrElse(requestUUID.toString, ListBuffer[LogRequests]()) += LogRequests(tbl, cnt, logSelectStmt))
		}
	}


	// Log requests to log table
	def logTableRequests(requestsMap: Map[UUID, RequestsMap], logRequestsMap: mutable.Map[String, ListBuffer[LogRequests]], tbl: String, activeTables: mutable.LinkedHashMap[String, TableProperties], searchKeySpace: Map[String, String]): Unit = {
		requestsMap.foreach { case (requestUuid, _) =>
			log.debug(s"Logging Table Requests for $tbl and request $requestUuid")

			val lr = if (logRequestsMap contains requestUuid.toString) {
				logRequestsMap(requestUuid.toString).find(_.tbl == tbl).getOrElse(LogRequests(tbl, 0, constructSelectStmt(activeTables(tbl).selectStmt, requestsMap, ForgetMeAPI.addObfuscateKeyword(activeTables(tbl).searchKey), searchKeySpace)))
			} else LogRequests(tbl, 0, constructSelectStmt(activeTables(tbl).selectStmt, requestsMap, ForgetMeAPI.addObfuscateKeyword(activeTables(tbl).searchKey), searchKeySpace))

			log.debug(s"Constructed Log Request")
			ForgetMeSqlOperations.executeUpdateStmt(ForgetMeSqlOperations.getInsertToLogTbl(requestUuid, lr.tbl, lr.cnt, lr.logStmt.replaceAll("'", "''")))

		}
	}


	def checkNulls(res: Any): String = if (res == null) "" else res.toString

}
