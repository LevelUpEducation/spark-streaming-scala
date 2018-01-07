package AdvancedStreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import util.Twitter

object UpdateStateByKey {
	def main(args: Array[String]): Unit = {
		val ssc = new StreamingContext("local[*]", "UpdateStateByKey", Seconds(5))

		ssc.checkpoint("checkpoints")

		Logger.getRootLogger.setLevel(Level.ERROR)

		Twitter.initialize()
		val languages = TwitterUtils.createStream(ssc, None)
			.filter(t => t.getLang != null)
			.map(t => (t.getLang, 1))

		languages.updateStateByKey(updateFunction)
			.foreachRDD(_.sortBy(_._2, ascending = false).foreach(println))

		ssc.start
		ssc.awaitTermination()
	}

	private def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
		val count = runningCount.getOrElse(0) + newValues.sum
		Some(count)
	}
}
