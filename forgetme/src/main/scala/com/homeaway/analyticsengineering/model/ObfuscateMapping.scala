package com.homeaway.analyticsengineering.model

/**
  * Created by aguyyala on 10/19/17.
  */


import java.time.Instant
import java.util.UUID
import com.homeaway.analyticsengineering.AnalyticsTaskApp.allConf


case class ObfuscateMapping(var registrationUuid: UUID = null, var requestUuid: UUID = null, var obfuscateMappingValues: Seq[ObfuscateMappingValues] = Seq(), var createDate: Instant = Instant.now(), var updateDate: Instant = Instant.now())

object ObfuscateMapping {

	import play.api.libs.functional.syntax._
	import play.api.libs.json._

	implicit val obfuscateMappingReads: Reads[ObfuscateMapping] = (
		(__ \ allConf("model.obfuscateMapping.registrationUuid")).read[String].map(UUID.fromString) and
			(__ \ allConf("model.obfuscateMapping.requestUuid")).read[String].map(UUID.fromString) and
			(__ \ allConf("model.obfuscateMapping.obfuscateMappingValues")).read[Seq[ObfuscateMappingValues]] and
			(__ \ allConf("model.obfuscateMapping.createDate")).read[Instant] and
			(__ \ allConf("model.obfuscateMapping.updateDate")).read[Instant]
		) (ObfuscateMapping.apply _)
}