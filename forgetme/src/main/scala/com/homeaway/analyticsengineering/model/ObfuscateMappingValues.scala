package com.homeaway.analyticsengineering.model

/**
  * Created by aguyyala on 10/19/17.
  */

import com.homeaway.analyticsengineering.AnalyticsTaskApp.allConf
import com.homeaway.datatech.forgetme.model.enums.MaskType


case class ObfuscateMappingValues(var valueToEncrypt: String = null, var maskType: MaskType = MaskType.STRING, var obfuscatedValue: String = null, var lengthLimitOfObfuscatedValue: Int = -1, var metadata: Map[String, Any] = null)

object ObfuscateMappingValues {

	import play.api.libs.functional.syntax._
	import play.api.libs.json._

	implicit val obfuscateMappingValuesReads: Reads[ObfuscateMappingValues] = (
		(__ \ allConf("model.obfuscateMappingValues.valueToEncrypt")).read[String] and
			(__ \ allConf("model.obfuscateMappingValues.maskType")).read[String].map(x => MaskType.valueOf(x)) and
			(__ \ allConf("model.obfuscateMappingValues.obfuscatedValue")).read[String] and
			(__ \ allConf("model.obfuscateMappingValues.lengthLimitOfObfuscatedValue")).read[Int] and
			(__ \ allConf("model.obfuscateMappingValues.metadata")).readNullable[JsValue].map(x =>
				x.orNull match {
					case JsObject(m) => m.toMap
					case _ => null
				})
		) (ObfuscateMappingValues.apply _)

	implicit val obfuscateMappingValuesWrites: Writes[ObfuscateMappingValues] = Writes[ObfuscateMappingValues](
		(obfuscateMappingValues: ObfuscateMappingValues) => Json.obj(
			allConf("model.obfuscateMappingValues.valueToEncrypt") -> obfuscateMappingValues.valueToEncrypt,
			allConf("model.obfuscateMappingValues.maskType") -> obfuscateMappingValues.maskType.toString,
			allConf("model.obfuscateMappingValues.obfuscatedValue") -> obfuscateMappingValues.obfuscatedValue,
			allConf("model.obfuscateMappingValues.lengthLimitOfObfuscatedValue") -> obfuscateMappingValues.lengthLimitOfObfuscatedValue,
			allConf("model.obfuscateMappingValues.metadata") -> (if (obfuscateMappingValues.metadata != null) obfuscateMappingValues.metadata.mapValues {
				case x: String => JsString(x)
				case x: Int => JsNumber(x)
				case x: Boolean => JsBoolean(x)
				case _ => JsNull
			} else Json.obj())
		)
	)
}
