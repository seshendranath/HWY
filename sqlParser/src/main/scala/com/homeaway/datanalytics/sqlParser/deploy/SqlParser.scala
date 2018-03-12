package com.homeaway.datanalytics.sqlParser.deploy

/**
  * Created by aguyyala on 6/6/17.
  */

import com.homeaway.datanalytics.sqlParser.config.ParserConfig
import org.apache.log4j.{Level, Logger}

object SqlParser {

  val log = Logger.getLogger(getClass)

  def main(args: Array[String]) {
    try {

      val parserConfig = ParserConfig.parseCmdLineArguments(args)

      log.setLevel(Level.toLevel(parserConfig.logLevel))

      log.debug(parserConfig)

      new SqlParserCore(parserConfig).runner()
    }
    catch {
      case e: Exception => errorHandler(e)
    }

  }

  def errorHandler(e: Exception) = {
    log.error(s"Something went WRONG during the run: ${e.printStackTrace()}")
    log.error("Exiting with FAILED status and exit code 1")
    System.exit(1)
  }


}
