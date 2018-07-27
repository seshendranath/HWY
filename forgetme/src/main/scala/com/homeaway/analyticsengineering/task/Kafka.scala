package com.homeaway.analyticsengineering.task

/**
  * Created by aguyyala on 10/19/17.
  */

import java.time.Instant
import java.util.Collections.singletonList
import java.util.{Properties, UUID}

import com.homeaway.analyticsengineering.AnalyticsTaskApp.conf
import com.homeaway.analyticsengineering.utilities.Logging
import com.homeaway.datatech.forgetme.model.ForgetmeKafkaMessageAck
import com.homeaway.datatech.forgetme.model.enums.ResponsesType
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import kafka.api.TopicMetadata


class Kafka extends Logging {

	val topic: String = conf.topic
	val ackTopic: String = conf.ackTopic

	val props = new Properties()
	props.put("bootstrap.servers", conf.bootstrapServers)
	props.put("key.deserializer", conf.keyDeserializer)
	props.put("value.deserializer", conf.valueDeserializer)
	props.put("key.serializer", conf.keySerializer)
	props.put("value.serializer", conf.valueSerializer)
	props.put("consumer.id", conf.productName)
	props.put("group.id", conf.productName)
	props.put("partition.assignment.strategy", conf.partitionAssignmentStrategy)
	props.put("auto.commit.interval.ms", conf.autoCommitIntervalMs)
	props.put("auto.offset.reset", conf.autoOffsetReset)
	props.put("schema.registry.url", conf.schemaRegistryUrl)


	val consumer = new KafkaConsumer[String, String](props)
	consumer.subscribe(singletonList(topic))

	val producer = new KafkaProducer[String, ForgetmeKafkaMessageAck](props)


	def sendAck(registrationUuid: UUID, requestUuid: UUID, responseType: ResponsesType, responseDescription: String): Unit = {

		val forgetMeAck = new ForgetmeKafkaMessageAck
		forgetMeAck.setRegistrationUuid(registrationUuid)
		forgetMeAck.setRequestUuid(requestUuid)
		forgetMeAck.setResponseDate(Instant.now())
		forgetMeAck.setResponseType(responseType.getValue)
		forgetMeAck.setResponseDescription(responseDescription)

		val record = new ProducerRecord[String, ForgetmeKafkaMessageAck](ackTopic, forgetMeAck)

		producer.send(record).get
	}


	def cleanup(): Unit = {
		consumer.close()
		producer.close()
	}


}
