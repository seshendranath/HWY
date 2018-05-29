package com.homeaway.analyticsengineering.task


/**
  * Created by aguyyala on 04/01/18.
  */


import java.net.URI
import java.util.UUID

import com.homeaway.analyticsengineering.model.{ColumnProperties, RegistrationDetails, RequestsMap, TableProperties}
import com.homeaway.analyticsengineering.AnalyticsTaskApp.conf
import com.homeaway.analyticsengineering.utilities.{Logging, Utility}
import com.homeaway.analyticsengineering.enums.SearchKeys
import com.homeaway.datatech.forgetme.client.jersey2.ForgetmeClient
import com.homeaway.datatech.forgetme.model
import com.homeaway.datatech.forgetme.model.{ObfuscateMapping, ObfuscateMappingValues, enums}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import util.control.Breaks._
import com.fasterxml.jackson.databind.ObjectMapper
import okhttp3.{OkHttpClient, Request}
import play.api.libs.json.{JsValue, Json, Reads}


object ForgetMeAPI extends Logging {

	val client = new OkHttpClient

	val forgetmeClient = new ForgetmeClient(client, new URI(conf.forgetMeUrl))

	val encryptableSearchKeys = Map(SearchKeys.EMAIL.toString -> enums.MaskType.EMAIL)

	/**
	  * Create a new Registration
	  */
	def newRegistration(): UUID = {
		val registration = new model.Registration()
		registration.setPortfolio(conf.portfolioName)
		registration.setPortfolioId(conf.portfolioId)
		registration.setProduct(conf.productName)
		registration.setProductId(conf.productId)
		registration.setDevEmail(conf.devEmail)
		registration.setDescription("This registration is for obfuscation of data.")

		val newRegistrationDetails = forgetmeClient.createRegistration(registration)
		newRegistrationDetails.getRegistrationUuid

	}


	/**
	  * 1. Makes the ForgetMe API call to get Registration UUID
	  * 2. ForgetMe gives back Registration UUID
	  */
	def getRegistrationUuid: UUID = {
		log.debug("Getting Registration UUID for Portfolio: %s and Product: %s".format(conf.portfolioName, conf.productName))
		val registrationDetails = forgetmeClient.findRegistrationsByPortfolio(conf.portfolioName, conf.productName).asScala.headOption.orNull
		log.debug("Fetching Registration UUID Successful")

		if (registrationDetails == null) {
			newRegistration()
		}
		else {
			registrationDetails.getRegistrationUuid
		}

	}


	/**
	  * Makes the ForgetMe API call to get Request details
	  */
	def getForgetMeRequest(requestUuid: UUID): model.Request = forgetmeClient.findByRequestUUID(requestUuid)


	/**
	  * Pretty Prints JSON Object
	  */
	def printJSONObject(obArray: List[ObfuscateMappingValues]): Unit = {
		log.debug(s"Obfuscating following record: $obArray")
		val mapper = new ObjectMapper()
		log.debug(obArray.map(mapper.writerWithDefaultPrettyPrinter.writeValueAsString(_)).mkString("\n,"))
	}


	/**
	  * Parse the results of a table and create a seq of ObfuscateMappingValues which we will submit to the
	  * ForgetMe API to get the obfuscated results
	  */
	def processResultsAndGetObArray(tbl: String, results: Map[String, Array[String]], colMaskTypeMap: Map[String, ColumnProperties]): List[ObfuscateMappingValues] = {

		log.info(s"Creating ObfuscateMappingValues Array for $tbl and $colMaskTypeMap")

		val obArray = ListBuffer[ObfuscateMappingValues]()

		results.foreach { case (col, values) =>
			val colPropMap = colMaskTypeMap.getOrElse(col, ColumnProperties(enums.MaskType.STRING, 25))
			val lengthLimitOfObfuscatedValue = conf.maskTypeLengths(colPropMap.maskType)

			values.foreach { v =>
				val ob = new ObfuscateMappingValues
				ob.setValueToEncrypt(v)
				ob.setMaskType(colPropMap.maskType)
				ob.setLengthLimitOfObfuscatedValue(lengthLimitOfObfuscatedValue)
				ob.setMetadata(Map("table" -> tbl, "column" -> col).asInstanceOf[Map[String, AnyRef]].asJava)
				obArray.append(ob)
			}
		}

		obArray.toList
	}


	/**
	 * Extract field name from Requests Map
	 */
	def extract(p: RequestsMap, fieldName: String): String = {
		val email = SearchKeys.EMAIL.toString
		val obfuscatedEmail = conf.obfuscatedKeyword + SearchKeys.EMAIL.toString.capitalize
		val userID = SearchKeys.USERID.toString
		val userUUID = SearchKeys.USERUUID.toString
		val accountUUID = SearchKeys.ACCOUNTUUID.toString

		fieldName match {
			case `email` => p.email
			case `obfuscatedEmail` => p.obfuscatedEmail
			case `userID` => p.userID
			case `userUUID` => p.userUUID
			case `accountUUID` => p.accountUUID
		}
	}


	def addObfuscateKeyword(searchKey: String): String = {
		if (encryptableSearchKeys contains searchKey) conf.obfuscatedKeyword + searchKey.capitalize
		else searchKey
	}


	/**
	  * Parse the results of a table for ALL REQUESTS AT ONCE and create a seq of ObfuscateMappingValues which we will submit to the
	  * ForgetMe API to get the obfuscated results
	  */
	def processResultsAndGetObArrayAtOnce(tbl: String, results: Map[String, Array[String]], props: TableProperties, requestsMap: Map[UUID, RequestsMap]): Map[String, mutable.Set[ObfuscateMappingValues]] = {

		val colMaskTypeMap: Map[String, ColumnProperties] = props.colMaskTypeMap
		log.info(s"Creating ObfuscateMappingValues Array for $tbl and $colMaskTypeMap")

		val requestsObfuscateMapping = mutable.Map[String, mutable.Set[ObfuscateMappingValues]]()

		results.filterKeys(_ != conf.requestUUIDCol).foreach { case (col, values) =>
			val colPropMap = colMaskTypeMap.getOrElse(col, ColumnProperties(enums.MaskType.STRING, 25))
			val lengthLimitOfObfuscatedValue = conf.maskTypeLengths(colPropMap.maskType)

			for ((v, i) <- values.zipWithIndex) {
				val ob = new ObfuscateMappingValues
				ob.setValueToEncrypt(v)
				ob.setMaskType(colPropMap.maskType)
				ob.setLengthLimitOfObfuscatedValue(lengthLimitOfObfuscatedValue)
				ob.setMetadata(Map("table" -> tbl, "column" -> col, "replaceKey" -> extract(requestsMap(UUID.fromString(results(conf.requestUUIDCol)(i))), props.searchKey)
					, "obfuscatedReplaceKey" -> extract(requestsMap(UUID.fromString(results(conf.requestUUIDCol)(i))), addObfuscateKeyword(props.searchKey))
				).asInstanceOf[Map[String, AnyRef]].asJava)

				requestsObfuscateMapping += results(conf.requestUUIDCol)(i) -> (requestsObfuscateMapping.getOrElse(results(conf.requestUUIDCol)(i), mutable.Set[ObfuscateMappingValues]()) += ob)
			}
		}

		requestsObfuscateMapping.toMap
	}


	/**
	  * Fetch Email Details of a Request
	  */
	def getEmailDetails(registrationUuid: UUID, requestType: String, requestUuid: UUID): (String, String, String) = {
		val request = try {
			getForgetMeRequest(requestUuid)
		} catch {
			case e: RuntimeException => e.printStackTrace()
				log.error(s"ERROR MESSAGE: ${e.getMessage}")
				log.error(s"Got Error while processing $requestType Request: $requestUuid")
				return ("", "", "")
		}

		val origEmail = request.getCustomerDetails.getEmail.toLowerCase

		val ob = new ObfuscateMappingValues
		ob.setValueToEncrypt(origEmail)
		ob.setMaskType(enums.MaskType.EMAIL)
		ob.setLengthLimitOfObfuscatedValue(conf.maskTypeLengths(enums.MaskType.EMAIL))
		ob.setMetadata(Map("table" -> "dummy", "column" -> "dummy").asInstanceOf[Map[String, AnyRef]].asJava)

		val completeObArray = getCompleteObArray(registrationUuid, requestType, requestUuid, List(ob))

		val obfuscateMapping = fetchObfuscatedresults(requestType, registrationUuid, requestUuid, completeObArray)

		val obfuscatedEmail = obfuscateMapping.getObfuscateMappingValues.asScala.filter(x => x.getValueToEncrypt == origEmail && x.getMaskType == enums.MaskType.EMAIL).head.getObfuscatedValue
		(origEmail, Utility.sha256Hash(origEmail), obfuscatedEmail)
	}


	/**
	  * Check if a request has any previously obfuscated values, if there are add it to the current seq of ObfuscationMappingValues
	  */
	def getCompleteObArray(registrationUuid: UUID, requestType: String, requestUuid: UUID, obArray: List[ObfuscateMappingValues]): List[ObfuscateMappingValues] = {
		val completeObArray = if (requestType == enums.RequestType.ForgetMe.getValue) {
			val existingObArray = forgetmeClient.findObfuscateWithValues(registrationUuid, requestUuid)
			if (existingObArray.isPresent && existingObArray.get.getObfuscateMappingValues != null) {
				(existingObArray.get.getObfuscateMappingValues.asScala.toSet ++ obArray.toSet).toList
			} else obArray
		} else obArray

		completeObArray
	}


	/**
	  * Get Obfuscated Results back for a request
	  */
	def fetchObfuscatedresults(requestType: String, registrationUuid: UUID, requestUuid: UUID, obArray: List[ObfuscateMappingValues] = List()): ObfuscateMapping = {
		if (requestType == enums.RequestType.ForgetMe.getValue) {
			printJSONObject(obArray)
			forgetmeClient.obfuscateWithValues(registrationUuid, requestUuid, obArray.asJava)
		} else forgetmeClient.deobfuscateWithValues(registrationUuid, requestUuid).get
	}


	/**
	  * Process obfuscated results and create a Map of original values to obfuscated values
	  */
	def processResultsAndGetMap(requestType: String, registrationUuid: UUID, requestUuid: UUID, tbl: String, results: Map[String, Array[String]]
	                            , colMaskTypeMap: Map[String, ColumnProperties], obfuscatedResults: ObfuscateMapping
	                            , encryptedSearchKeySpace: Map[String, String], replaceKeyword: String, replaceKeyMaskType: enums.MaskType): (String, List[Map[String, String]]) = {

		log.info(s"Fetching Results for $requestType, $requestUuid, $tbl, $colMaskTypeMap")

		log.debug(s"Obfuscated Results: $obfuscatedResults")

		val obResults = mutable.Map[String, Array[String]]()

		results.foreach { case (col, values) =>
			val colPropMap = colMaskTypeMap.getOrElse(col, ColumnProperties(enums.MaskType.STRING, conf.maskTypeLengths(enums.MaskType.STRING)))

			val encryptedValues = ArrayBuffer[String]()
			values.foreach { v =>
				breakable {
					obfuscatedResults.getObfuscateMappingValues.asScala.foreach { r =>
						val metaCol = r.getMetadata.asScala.getOrElse("column", "").toString
						val metaTbl = r.getMetadata.asScala.getOrElse("table", "").toString

						val metaMaskType = r.getMaskType
						val valueToEncrypt = r.getValueToEncrypt
						val obfuscatedValue = r.getObfuscatedValue

						if (metaTbl == tbl && metaCol == col && metaMaskType == colPropMap.maskType) {
							if (requestType == enums.RequestType.ForgetMe.getValue) {
								log.debug(s"Comparing $v and $valueToEncrypt for $requestType and $requestUuid")
								if (v == valueToEncrypt) {
									encryptedValues.append(obfuscatedValue)
									break
								}

							}
							else {
								log.debug(s"Comparing $v and ${obfuscatedValue.toLowerCase} for $requestType and $requestUuid")
								if (v == obfuscatedValue.toLowerCase) {
									encryptedValues.append(valueToEncrypt)
									break
								}

							}
						}

					}
				}
			}

			obResults += col -> encryptedValues.toArray
		}

		log.debug(s"Obfuscated Results complete for $tbl, $requestType and $requestUuid: ${obResults.mapValues(_.mkString(","))}")
		log.debug(s"Parsing Obfuscated Results complete for $tbl, results for $requestType and $requestUuid: ${results.mapValues(_.mkString(","))}")

		val finalResults = ListBuffer[Map[String, String]]()
		val n = results.values.head.length
		val cols = results.keys
		for (i <- 0 until n) {
			val tmpResults = mutable.Map[String, String]()
			cols.foreach { c =>
				log.debug(s"processing Results for column $c and index $i")
				tmpResults += c -> results(c)(i)
				tmpResults += c + conf.obfuscatedKeyword -> obResults(c)(i)
			}
			finalResults.append(tmpResults.toMap)
		}

		log.debug(s"Finding Replace key for ${encryptedSearchKeySpace(replaceKeyword.replace(conf.obfuscatedKeyword, "")).toLowerCase} in ${obfuscatedResults.getObfuscateMappingValues.asScala}")
		val replaceKey = if (!(replaceKeyword contains conf.obfuscatedKeyword)) encryptedSearchKeySpace(replaceKeyword)
		else obfuscatedResults.getObfuscateMappingValues.asScala.filter(x => x.getMaskType == replaceKeyMaskType && x.getValueToEncrypt.replaceAll("'", "''") == encryptedSearchKeySpace(replaceKeyword.replace(conf.obfuscatedKeyword, ""))).head.getObfuscatedValue.toLowerCase


		log.debug(s"Final Obfuscated Results: $finalResults")
		(replaceKey, finalResults.toList)
	}


	/**
	  * If there are any Search Keys that can be or needs to be obfuscated. Create a Map of original and obfuscated Search Keys.
	  */
	def getObfuscatedValuesMap(registrationUuid: UUID, requestUuid: UUID, searchKeySpace: Map[String, String], searchKey: Map[String, enums.MaskType]): Map[String, String] = {

		log.debug(s"Fetching obfuscated results")
		val obfuscatedResults = forgetmeClient.deobfuscateWithValues(registrationUuid, requestUuid)

		if (obfuscatedResults.isPresent && obfuscatedResults.get.getObfuscateMappingValues != null) {

			log.debug(s"Got back obfuscatedResults Map $obfuscatedResults")
			val newSpace = mutable.Map[String, String]()
			searchKey.foreach { case (sk, mk) =>
				obfuscatedResults.get.getObfuscateMappingValues.asScala.foreach { r =>
					val valueToEncrypt = r.getValueToEncrypt.replaceAll("'", "''")
					val maskType = r.getMaskType

					if (searchKeySpace(sk) == valueToEncrypt && maskType == mk) {
						newSpace += ((sk + conf.obfuscatedKeyword) -> r.getObfuscatedValue.toLowerCase)
					}
				}
			}
			newSpace.toMap ++ searchKeySpace
		} else searchKeySpace
	}


	/**
	  * Get Request Status by requestUUID
	  */
	def getRequestStatus(requestUUID: String): String = {
		try {
			val request = new Request.Builder()
				.url(s"${conf.forgetMeUrl}/v1/requests/log/$requestUUID")
				.get

			val response = client.newCall(request.build).execute
			val result = Json.parse(new String(response.body.bytes))

			implicit val registrationDetailsReader: Reads[RegistrationDetails] = Json.reads[RegistrationDetails]
			(result \ "request" \ "registrations").as[JsValue].as[List[RegistrationDetails]].filter(_.registrationUuid == conf.registrationUuid).head.responseType
		}
		catch {
			case _@(_: Exception | _: Error) => ""
		}
	}


	/**
	  * Check if the Request has any original values to be obfuscated
	  */
	def getRequestStatusByValues(requestType: String, requestUUID: UUID): Int = {
		try {
			val obfuscateMapping = if (requestType == enums.RequestType.ForgetMeNot.getValue) {
				forgetmeClient.deobfuscateWithValues(conf.registrationUuid, requestUUID)
			} else forgetmeClient.findObfuscateWithValues(conf.registrationUuid, requestUUID)

			if (obfuscateMapping.isPresent) {
				val obfuscateMappingValues = obfuscateMapping.get.getObfuscateMappingValues
				if (obfuscateMappingValues != null && obfuscateMappingValues.asScala.nonEmpty && obfuscateMappingValues.asScala.exists(_.getMetadata.containsKey("replaceKey"))) 1
				else 0
			} else 0
		} catch {
			case _@(_: Exception | _: Error) => 0
		}
	}

}