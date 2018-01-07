package AdvancedStreaming.solutions

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import util.Twitter

object StreamJoinSolution {
	def main(args: Array[String]): Unit = {
		val ssc = new StreamingContext("local[*]", "StreamJoinExercise", Seconds(10))

		Logger.getRootLogger.setLevel(Level.ERROR)

		Twitter.initialize()
		val hashtags = TwitterUtils.createStream(ssc, None)
	   	.filter(t => t.getHashtagEntities != null && t.getHashtagEntities.length > 0 && t.getLang == "en")
			.map(t => ("#" + t.getHashtagEntities.head.getText, 1))

		val shortStream = hashtags.window(Seconds(30))
	   	.reduceByKey(_ + _)

		val longStream = hashtags.window(Minutes(3))
			.reduceByKey(_ + _)

		shortStream.join(longStream)
			.transform(_.sortBy(t => t._2._1 * 100 + t._2._2, ascending = false))
			.print

		ssc.start
		ssc.awaitTermination()
	}
}
