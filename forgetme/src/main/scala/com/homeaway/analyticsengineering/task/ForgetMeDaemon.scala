package com.homeaway.analyticsengineering.task


/**
  * Created by aguyyala on 04/01/18.
  */


/*
 * ForgetMe Daemon runs in real time unless interrupted from outside.
 */
class ForgetMeDaemon {

	def run(): Unit = {
		val forgetMeQueue = new ForgetMeQueue
		val forgetMeRequestProcessorAtOnce = new ForgetMeRequestProcessorAtOnce

		while (true) {
			forgetMeQueue.run()
			forgetMeRequestProcessorAtOnce.run()
		}
	}
}
