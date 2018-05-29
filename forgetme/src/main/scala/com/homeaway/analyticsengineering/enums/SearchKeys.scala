package com.homeaway.analyticsengineering.enums


/**
  * Created by aguyyala on 04/01/18.
  */


object SearchKeys extends Enumeration {
	type SearchKeys = Value
	val EMAIL: Value = Value("email")
	val USERID: Value = Value("userid")
	val USERUUID: Value = Value("useruuid")
	val ACCOUNTUUID: Value = Value("accountuuid")
}