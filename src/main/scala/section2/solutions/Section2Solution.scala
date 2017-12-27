package section2.solutions

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import util.Twitter

object Section2Solution {

	def main(args: Array[String]): Unit = {
		Twitter.initialize()

		val ssc = new StreamingContext("local[*]", "Section2Exercise", Seconds(5))

		Logger.getRootLogger.setLevel(Level.ERROR)

		val tweets = TwitterUtils.createStream(ssc, None)

		ssc.checkpoint("checkpoints")

		tweets
			.window(Seconds(300), Seconds(10))
			.filter(t => t.getGeoLocation != null && t.getPlace != null)
			.map(t => (t.getGeoLocation.getLatitude, t.getGeoLocation.getLongitude, t.getPlace.getFullName))
			.transform{rdd =>
				if (rdd.count() > 0) {
					// calculate average latitude and longitude,
					val avgLatitude = rdd.map(_._1).reduce(_ + _) / rdd.count().toDouble
					val avgLongitude = rdd.map(_._2).reduce(_ + _) / rdd.count().toDouble

					println(s"Filtered tweet locations in the northeast of average latitude of $avgLatitude and average longitude of $avgLongitude:")

					// print name of place of tweets that are in the top-right sector of the area separated by the average
					rdd.filter(t => t._1 > avgLatitude && t._2 > avgLongitude).map(_._3)
				} else rdd.map(_._3)
			}
			.countByValueAndWindow(Seconds(300), Seconds(10)) // .countByValue() would work as well
	   	.foreachRDD(_.foreach(println))

		ssc.start
		ssc.awaitTermination()
	}
}
