package com.homeaway.analyticsengineering.task


/**
  * Created by aguyyala on 04/01/18.
  */


import com.homeaway.analyticsengineering.utilities.Logging
import com.homeaway.analyticsengineering.AnalyticsTaskApp.{conf, errorHandler}
import com.homeaway.datatech.forgetme.model.{ForgetmeKafkaMessage, enums}
import scala.collection.JavaConverters._


class ForgetMeQueue extends Logging {

	def run() {

		val kafka = new Kafka

		try {

			val registrationUuid = conf.registrationUuid // ForgetMe.getRegistrationUuid
			log.info("Registration UUID: " + registrationUuid)
			log.info("Reading Kafka Data")

			if (conf.fromBeginning) {
				kafka.consumer.poll(0)
				kafka.consumer.seekToBeginning(kafka.consumer.assignment())
			}

			var size = Int.MaxValue
			while (size > 0) {
				val records = kafka.consumer.poll(10000)
				size = records.count()
				log.info("Records Size: " + size)

				for (record <- records.asScala) {
					val value = record.value().asInstanceOf[ForgetmeKafkaMessage]

					val requestUuid = value.getRequestUuid
					val requestType = value.getRequestType

					kafka.sendAck(registrationUuid, requestUuid, enums.ResponsesType.INPROGRESS, enums.ResponsesType.INPROGRESS.getValue)

					log.info(s"Logging $requestType: $requestUuid into Queue table")
					ForgetMeSqlOperations.logRequestToQueue(requestUuid, requestType, enums.ResponsesType.INPROGRESS.getValue)
				}
			}
		}
		catch {
			case e@(_: Exception | _: Error) => errorHandler(e)
		}
		finally {
			kafka.cleanup()
		}

		log.info("END")

	}
}
