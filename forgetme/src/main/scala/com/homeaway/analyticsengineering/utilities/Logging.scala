package com.homeaway.analyticsengineering.utilities


/**
  * Created by aguyyala on 2/16/17.
  */


import com.homeaway.analyticsengineering.AnalyticsTaskApp.conf
import org.apache.log4j.{Level, Logger}

trait Logging {
	val log: Logger = Logger.getLogger(getClass)
	log.setLevel(Level.toLevel("Info"))

	if (conf.debug) log.setLevel(Level.toLevel("Debug"))

}
