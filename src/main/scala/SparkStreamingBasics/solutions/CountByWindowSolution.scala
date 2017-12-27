package SparkStreamingBasics.exercises

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import util.Twitter

object CountByWindowSolution {

	/**
	  * Exercise:
	  *
	  * Count and print the number of hashtags within a 5 second window, using the countByWindow() function.
	  * Update the count every three seconds.
	  *
	  * @param args
	  */
	def main(args: Array[String]): Unit = {
		Twitter.initialize()

		val ssc = new StreamingContext("local[*]", "CountByWindow", Seconds(1))

		Logger.getRootLogger.setLevel(Level.ERROR)

		val tweets = TwitterUtils.createStream(ssc, None)

		ssc.checkpoint("checkpoints")

		tweets.filter(_.getHashtagEntities != null)
			.map(_.getHashtagEntities.length)
			.countByWindow(Seconds(5), Seconds(3))
			.print

		ssc.start
		ssc.awaitTermination()
	}

}
