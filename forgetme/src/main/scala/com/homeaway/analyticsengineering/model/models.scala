package com.homeaway.analyticsengineering.model


/**
  * Created by aguyyala on 04/01/18.
  */


import java.util.UUID

import com.homeaway.datatech.forgetme.model.enums.MaskType


case class RequestsToProcess(requestType: String, requestUuid: UUID)

case class RequestsMap(requestType: String, email: String, obfuscatedEmail: String, sha256Email: String, userID: String, userUUID: String, accountUUID: String)

case class ColumnProperties(maskType: MaskType, lengthLimitOfObfuscatedValue: Int)

case class TableProperties(cols: Array[String], selectStmt: String, updateStmt: String, searchKey: String, colMaskTypeMap: Map[String, ColumnProperties])

case class Values(requestType: String, replaceKey: String, originalValue: String, replaceValue: String)

case class LogRequests(tbl: String, cnt: Int, logStmt: String)

case class RegistrationDetails(registrationUuid: String, responseType: String)