package com.homeaway.analyticsengineering


/**
  * Created by aguyyala on 04/01/18.
  */


import com.homeaway.analyticsengineering.config.AnalyticsTaskConfig.Conf
import config.AnalyticsTaskConfig
import org.apache.log4j.{Level, Logger}

import scala.io.Source


/*
 * Main class and entry point to the application
 */
object AnalyticsTaskApp extends App {

	val log: Logger = Logger.getLogger(getClass)
	log.setLevel(Level.toLevel("Info"))

	if (conf.debug) log.setLevel(Level.toLevel("Debug"))

	lazy val (conf: Conf, allConf: Map[String, String]) = AnalyticsTaskConfig.parseCmdLineArguments(args)

	Source.fromInputStream(getClass.getResourceAsStream("/banner.txt")).getLines.foreach(println)

	val s = System.nanoTime()

	try {

		val classes = conf.classes
		classes.foreach { cName =>
			val clazz = getClass.getClassLoader.loadClass(cName)
			clazz.getMethod("run").invoke(clazz.newInstance)
		}
	}
	catch {
		case e@(_: Exception | _: Error) => errorHandler(e)
	}

	val e = System.nanoTime()
	val totalTime = (e - s) / (1e9 * 60)
	log.info("Total Elapsed time: " + f"$totalTime%2.2f" + " mins")
	System.exit(0)


	def errorHandler(e: Throwable): Unit = {
		log.error(s"Something went WRONG during the run for Instance")
		log.error(s"${e.printStackTrace()}")
		System.exit(1)
	}

}
