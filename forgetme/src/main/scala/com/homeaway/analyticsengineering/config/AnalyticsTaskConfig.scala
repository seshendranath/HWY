package com.homeaway.analyticsengineering.config


/**
  * Created by aguyyala on 2/16/17.
  */


import java.util.UUID

import com.homeaway.datatech.forgetme.model.enums.MaskType
import scopt.OptionParser
import com.typesafe.config.ConfigFactory
import org.json4s.{DefaultFormats, Extraction}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source
import scala.util.matching.Regex


object AnalyticsTaskConfig {

	def parseCmdLineArguments(args: Array[String]): (Conf, Map[String, String]) = {

		val parser = new OptionParser[baseConfig]("Job") {
			head("Job")

			opt[String]('e', "env").required.action((x, c) => c.copy(env = x))
				.validate(x =>
					if (Seq("local", "dev", "test", "stage", "prod") contains x) success
					else failure("Invalid env: only takes dev, stage, or prod"))
				.text("Environment in which you want to run dev|stage|prod")

			opt[String]("sqlserver.pwd").required.text("SqlServer's Password - This is Mandatory and has to be passed from outside")
			opt[String]("app").required.text("Which specific Application is running the process. Ex: --app=ae")
			opt[String]("class").text("Specific Classes to run. Ex: --class=com.example.task1,com.example.task2")

			opt[Unit]("fromBeginning").text("If you want to read Kafka Topic From Beginning")

			opt[Unit]("debug").action { (_, c) =>
				c.copy(debug = "true")
			}.text("Debug Flag")

			help("help").text("Prints Usage Text")

			override def showUsageOnError = true

			override def errorOnUnknownArgument = false

			override def reportError(msg: String): Unit = {
				showUsageAsError
				Console.err.println("Error: " + msg)
				terminate(Left(msg))
			}

			override def reportWarning(msg: String): Unit = {}
		}

		val pConf = parser.parse(args, baseConfig()).getOrElse(baseConfig())
		if (pConf.debug == "true") pConf.logLevel = "Debug"

		var conf = mutable.Map(Extraction.decompose(pConf)(DefaultFormats).values.asInstanceOf[Map[String, String]].toSeq: _*)

		def getConf(confFile: String): Map[String, String] = ConfigFactory.parseResourcesAnySyntax(confFile).entrySet.asScala.map(e => (e.getKey, e.getValue.unwrapped.toString)).toMap

		Source.fromInputStream(getClass.getResourceAsStream("/loadConf.txt")).getLines.foreach { confFile =>
			conf ++= getConf(confFile)
		}

		val otherProperties = args.map(x => x.split("=", 2)).map {
			case Array(a, b) => (a.toString.stripPrefix("--"), b)
			case Array(a) => (a.toString.stripPrefix("--"), "true")
		}.toMap

		if (pConf.env == "local") {
			val sparkLocalProperties = getConf("spark-local.properties")
			conf ++= sparkLocalProperties
		}

		conf ++= otherProperties

		val convert = Map("env" -> pConf.env)

		implicit class StringImprovements(val s: String) {
			def richFormat(replacement: Map[String, Any]): String =
				(s /: replacement) { (res, entry) => res.replaceAll("#\\{%s\\}".format(entry._1), entry._2.toString) }
		}

		val allConf = conf.map { case (k, v) => k -> (v richFormat convert) }.toMap

		val nConf = Conf(allConf("bootstrapServers"),
			allConf("keySerializer"),
			allConf("valueSerializer"),
			allConf("keyDeserializer"),
			allConf("valueDeserializer"),
			allConf("partitionAssignmentStrategy"),
			allConf("autoCommitIntervalMs"),
			allConf("autoOffsetReset"),
			allConf("schemaRegistryUrl"),
			allConf("topic"),
			allConf("ackTopic"),
			allConf("devEmail"),
			allConf("obfuscatedKeyword"),
			allConf(s"forgetMe.${allConf("env")}.url"),
			allConf(s"${allConf("app")}.jobName"),
			allConf(s"${allConf("app")}.portfolioName"),
			allConf(s"${allConf("app")}.portfolioId"),
			allConf(s"${allConf("app")}.productId"),
			allConf(s"${allConf("app")}.productName"),
			allConf(s"${allConf("app")}.forgetMeQueueTbl"),
			allConf(s"${allConf("app")}.PIIMetadataTbl"),
			allConf(s"${allConf("app")}.obfuscationLogTbl"),
			UUID.fromString(allConf(s"${allConf("app")}.${allConf("env")}.registrationUuid")),
			allConf("class").split(",").map(_.trim),
			allConf.getOrElse("fromBeginning", "false").toBoolean,
			allConf("maskTypeLengths").split(",").map(_.trim).map { x => val y = x.split(":").map(_.trim); (MaskType.getMaskTypeByValue(y.head), y.last.toInt) }.toMap,
			allConf("env"),
			allConf("debug").toBoolean,
			allConf("logLevel"),
			reprocess = allConf.getOrElse("reprocess", "false").toBoolean,
			reprocessDays = allConf.getOrElse("reprocessDays", "7").toInt,
			softCheck = allConf.getOrElse("softCheck", "false").toBoolean
		)

		(nConf, allConf)
	}


	case class baseConfig(env: String = "stage", debug: String = "false", var logLevel: String = "Info")

	case class Conf(
		               bootstrapServers: String,
		               keySerializer: String,
		               valueSerializer: String,
		               keyDeserializer: String,
		               valueDeserializer: String,
		               partitionAssignmentStrategy: String,
		               autoCommitIntervalMs: String,
		               autoOffsetReset: String,
		               schemaRegistryUrl: String,
		               topic: String,
		               ackTopic: String,
		               devEmail: String,
		               obfuscatedKeyword: String,
		               forgetMeUrl: String,
		               jobName: String,
		               portfolioName: String,
		               portfolioId: String,
		               productId: String,
		               productName: String,
		               forgetMeQueueTbl: String,
		               PIIMetadataTbl: String,
		               obfuscationLogTbl: String,
		               registrationUuid: UUID,
		               classes: Array[String],
		               fromBeginning: Boolean,
		               maskTypeLengths: Map[MaskType, Int],
		               env: String,
		               debug: Boolean,
		               logLevel: String,
		               reprocess: Boolean,
		               reprocessDays: Int,
		               updatePattern: Regex = "(update\\s+)(\\w+)(\\s+set\\s+)(.*)(\\s+from\\s+.*)".r,
		               substitutePattern: Regex = "#\\{(\\w+)\\}".r,
		               softCheck: Boolean = false,
		               selectPattern: Regex = "(SELECT DISTINCT )(.*)(from .*)(where\\s+([\\w\\.]+)\\s*=\\s*'#\\{(\\w+)\\}')".r,
		               requestUUIDCol: String =  "RequestUUID"
	               )

}
