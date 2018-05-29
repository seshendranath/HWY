package com.homeaway.analyticsengineering.utilities


/**
  * Created by aguyyala on 04/01/18.
  */


import com.homeaway.analyticsengineering.AnalyticsTaskApp.allConf


object Utility extends Logging {
	def getConf(appName: String, rdb: String, env: String, allConfig: String, elseCase: String = null): String = {

		val allConfValue =
			try {
				allConf.getOrElse(s"$appName.$rdb.$env.$allConfig"
					, allConf.getOrElse(s"$appName.$rdb.$allConfig"
						, allConf.getOrElse(s"$appName.$env.$allConfig"
							, allConf.getOrElse(s"$appName.$allConfig"
								, allConf.getOrElse(s"$rdb.$env.$allConfig"
									, allConf.getOrElse(s"$rdb.$allConfig"
										, allConf(s"$allConfig")))))))
			}

			catch {
				case _: NoSuchElementException => if (elseCase ne null) elseCase else throw new NoSuchElementException(allConfig)
			}

		allConfValue
	}


	def timeit(s: Long, msg: String): Unit = {
		val e = System.nanoTime()
		val totalTime = (e - s) / (1e9 * 60)
		log.info(msg + " " + f"$totalTime%2.2f" + " mins")
	}

	def sha256Hash(text: String): String = String.format("%064x", new java.math.BigInteger(1, java.security.MessageDigest.getInstance("SHA-256").digest(text.getBytes("UTF-8"))))
}
