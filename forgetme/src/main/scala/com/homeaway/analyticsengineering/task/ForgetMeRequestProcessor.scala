package com.homeaway.analyticsengineering.task


/**
  * Created by aguyyala on 04/01/18.
  */


import java.util.UUID

import com.homeaway.analyticsengineering.AnalyticsTaskApp.conf
import com.homeaway.analyticsengineering.model.{RequestsToProcess, TableProperties}
import com.homeaway.analyticsengineering.utilities.{Logging, Utility}
import com.homeaway.datatech.forgetme.model.{ObfuscateMappingValues, UserAccount, enums}
import com.homeaway.analyticsengineering.enums.SearchKeys
//import com.microsoft.sqlserver.jdbc.SQLServerException

import util.control.Breaks._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer


class ForgetMeRequestProcessor extends Logging {

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

		val kafka = new Kafka
		requestsToProcess.foreach { req =>
			val found = processRequest(req.requestType, req.requestUuid, activeTables)

			val (_, sha256Email, obfuscatedEmail) = ForgetMeAPI.getEmailDetails(registrationUuid, req.requestType, req.requestUuid)

			if (found) {
				log.info(s"Updating Request Status for $req to COMPLETED")
				ForgetMeSqlOperations.updateRequestStatus(req.requestType, req.requestUuid, enums.ResponsesType.COMPLETED.getValue, sha256Email, obfuscatedEmail)
				log.info(s"Sending Request Status for $req as COMPLETED to Kafka")
				kafka.sendAck(registrationUuid, req.requestUuid, enums.ResponsesType.COMPLETED, enums.ResponsesType.COMPLETED.getValue)
			}
			else {
				log.info(s"Updating Request Status for $req to NOT FOUND")
				ForgetMeSqlOperations.updateRequestStatus(req.requestType, req.requestUuid, enums.ResponsesType.NOTFOUND.getValue, sha256Email, obfuscatedEmail)
				log.info(s"Sending Request Status for $req as NOT FOUND to Kafka")
				kafka.sendAck(registrationUuid, req.requestUuid, enums.ResponsesType.NOTFOUND, enums.ResponsesType.NOTFOUND.getValue)
			}

		}

		log.info("ALL REQUESTS PROCESSED")
	}


	def checkNulls(res: Any): String = if (res == null) "null" else res.toString


	def addObfuscateKeyword(requestType: String, searchKey: String, obfuscateSearchKeys: Map[String, enums.MaskType], lite: Boolean = false): String = {
		if (!lite) {
			if (requestType == enums.RequestType.ForgetMeNot.getValue && obfuscateSearchKeys.contains(searchKey)) searchKey + conf.obfuscatedKeyword
			else searchKey
		} else {
			if (obfuscateSearchKeys.contains(searchKey)) searchKey + conf.obfuscatedKeyword
			else searchKey
		}
	}


	def processRequest(requestType: String, requestUuid: UUID, activeTables: mutable.LinkedHashMap[String, TableProperties]): Boolean = {

		log.info(s"Processing Request $requestType: $requestUuid")

		log.info(s"Getting request $requestUuid")
		val request = try {
			ForgetMeAPI.getForgetMeRequest(requestUuid)
		} catch {
			case e: RuntimeException => e.printStackTrace()
				log.error(s"ERROR MESSAGE: ${e.getMessage}")
				log.error(s"Got Error while processing $requestType Request: $requestUuid")
				return false
		}

		log.debug("Fetching Email")
		val email = request.getCustomerDetails.getEmail.toLowerCase

		log.debug("Fetching User ID")
		val userID = checkNulls(request.getCustomerDetails.getUserID)

		log.debug("Fetching User UUID")
		val userUUID = checkNulls(request.getCustomerDetails.getUserUUID)

		log.debug("Fetching Account UUID")
		val userAccounts = request.getCustomerDetails.getUserAccounts
		val accountUUID = if (userAccounts == null) "null" else checkNulls(userAccounts.asScala.headOption.getOrElse(new UserAccount).getAccountUUID)

		val searchKeySpace: Map[String, String] = Map(
			SearchKeys.EMAIL.toString -> email.replaceAll("'", "''")
			, SearchKeys.USERID.toString -> userID
			, SearchKeys.USERUUID.toString -> userUUID
			, SearchKeys.ACCOUNTUUID.toString -> accountUUID
		)

		val obfuscateSearchKeys = Map(SearchKeys.EMAIL.toString -> enums.MaskType.EMAIL)

		log.debug(s"Completed Fetching all Search Keys")
		var found = false
		val encryptedSearchKeySpace = if (requestType == enums.RequestType.ForgetMeNot.getValue) {
			val newSpace = ForgetMeAPI.getObfuscatedValuesMap(registrationUuid, requestUuid, searchKeySpace, obfuscateSearchKeys)
			if (newSpace == searchKeySpace) {
				log.info(s"Obfuscation NOT DONE on Request: $requestType and $requestUuid")
				return found
			}
			newSpace
		} else searchKeySpace

		log.debug(s"Encrypted Search Key Space: $encryptedSearchKeySpace")

		val obArray = ListBuffer[ObfuscateMappingValues]()
		val tablesContainingRequest = mutable.LinkedHashMap[String, Map[String, Array[String]]]()

		for ((tbl, props) <- activeTables) {

			breakable {
				log.info(s"Processing request $requestType and $requestUuid for $tbl and $props")
				val searchKey = props.searchKey
				val selectStmt = props.selectStmt.replace(s"#{$searchKey}", encryptedSearchKeySpace(addObfuscateKeyword(requestType, searchKey, obfuscateSearchKeys)))

				val cnt = ForgetMeSqlOperations.checkIfAlreadyProcessed(requestUuid, tbl)

				if ((!(obfuscateSearchKeys contains searchKey) && cnt > 0) || (conf.softCheck && cnt > 0)) {
					log.info(s"Request $requestType $requestUuid ALREADY Processed/Obfuscated/DeObfuscated for Table $tbl")
					break
				}

				if (props.cols.isEmpty) {
					log.info(s"Plain Update Statement with no columns to obfuscate")
					val recordCnt = ForgetMeSqlOperations.executeUpdateStmt(props.updateStmt.replace(s"#{$searchKey}", encryptedSearchKeySpace(addObfuscateKeyword(requestType, searchKey, obfuscateSearchKeys))))
					if (recordCnt > 0) found = true
					break
				}

				val s = System.nanoTime()
				val results: Map[String, Array[String]] = ForgetMeSqlOperations.executeSelectStmt(selectStmt, props.cols)
				Utility.timeit(s, s"Run Time of SELECT for $tbl")
				log.debug(s"Got Results from Select Stmt for $tbl")

				if (results.isEmpty) {
					log.info(s"No Results FOUND for $requestUuid for Table $tbl")
					ForgetMeSqlOperations.executeUpdateStmt(ForgetMeSqlOperations.getInsertToLogTbl(requestUuid, tbl, 0))
					break
				}

				found = true
				tablesContainingRequest += (tbl -> results)
				obArray ++= ForgetMeAPI.processResultsAndGetObArray(tbl, results, props.colMaskTypeMap)
			}
		}

		if (!found) return found

		val completeObArray = ForgetMeAPI.getCompleteObArray(registrationUuid, requestType, requestUuid, obArray.toList)

		val obfuscateMapping = ForgetMeAPI.fetchObfuscatedresults(requestType, registrationUuid, requestUuid, completeObArray)

		tablesContainingRequest.foreach { case (tbl, results) =>
			val props = activeTables(tbl)
			val searchKey = props.searchKey

			val replaceKeyword = addObfuscateKeyword(requestType, searchKey, obfuscateSearchKeys, lite = true)

			val (replaceKey, obfuscatedResults) = ForgetMeAPI.processResultsAndGetMap(requestType, registrationUuid, requestUuid, tbl, results, props.colMaskTypeMap, obfuscateMapping, encryptedSearchKeySpace, replaceKeyword, obfuscateSearchKeys.getOrElse(searchKey, enums.MaskType.STRING))

			val selectStmt = props.selectStmt.replace(s"#{$searchKey}", replaceKey)
			val updateStmt = props.updateStmt.replace(s"#{$searchKey}", encryptedSearchKeySpace(addObfuscateKeyword(requestType, searchKey, obfuscateSearchKeys)))

			implicit class StringImprovements(val s: String) {
				def richFormat(replacement: Map[String, Any]): String =
					(s /: replacement) { (res, entry) => res.replaceAll("#\\{%s\\}".format(entry._1), entry._2.toString.replaceAll("'", "''")) }
			}

			var newUpdatedStmt: String = updateStmt

			log.debug(s"Replacing the following List of records $obfuscatedResults")
			obfuscatedResults.foreach { m =>

				val executeUpdatedStmt = newUpdatedStmt.richFormat(m)
				try {
					val s = System.nanoTime()
					ForgetMeSqlOperations.executeUpdateStmt(executeUpdatedStmt)
					Utility.timeit(s, s"Run Time of UPDATE for $tbl")
				} catch {
					case e@(_: Exception | _: Error) => throw e
					//					case e: SQLServerException =>
					//						log.error(s"FOUND SQLServerException while processing $tbl")
					//						log.error(e.getMessage)
					//
					//						if (e.getMessage contains "Cannot insert duplicate key row") {
					//							val dupPattern = ".*The duplicate key value is \\((.*)\\).*".r
					//							val dupPattern(duplicateKey) = e.getMessage
					//							log.error(s"FOUND Duplicate Key for $tbl, $searchKey, $duplicateKey")
					//
					//							val selectPattern = "SELECT.*\\s+(from\\s+[\\w\\.]+\\s+(\\w+)\\s+.*)".r
					//							val selectPattern(from, alias) = selectStmt
					//							val executeDeleteStmt = s"DELETE $alias $from"
					//
					//							var s = System.nanoTime()
					//							//							ForgetMeSqlOperations.executeUpdateStmt(executeDeleteStmt)
					//							Utility.timeit(s, s"Run Time of DELETE for $tbl")
					//
					//							s = System.nanoTime()
					//							//							ForgetMeSqlOperations.executeUpdateStmt(executeUpdatedStmt)
					//							Utility.timeit(s, s"Run Time of UPDATE for $tbl")
					//						}
				}


				log.debug(s"Replaced Updated Statement for $requestType and $requestUuid: $executeUpdatedStmt")

				newUpdatedStmt = updateStmt
			}

			ForgetMeSqlOperations.executeUpdateStmt(ForgetMeSqlOperations.getInsertToLogTbl(requestUuid, tbl, obfuscatedResults.length, selectStmt.replaceAll("'", "''")))
		}

		found
	}

}
