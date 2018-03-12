package com.homeaway.analyticsengineering.encrypt.main

import com.homeaway.analyticsengineering.encrypt.secret.Decrypt
import org.apache.log4j.{Level, Logger}


object Encrypt extends App {
	val log = Logger.getLogger(getClass)
	log.setLevel(Level.toLevel("Info"))

	log.info(Decrypt.getEncryptedPWD(args(0)))

}
