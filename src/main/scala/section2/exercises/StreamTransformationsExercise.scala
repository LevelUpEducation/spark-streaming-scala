package section2.exercises

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import util.Twitter

object StreamTransformationsExercise {

	/**
	  * Exercise:
	  *
	  * Determine the number of tweets that are not originals but retweets and print the number
	  * of retweets in each streaming window.
	  *
	  * @param args
	  */
	def main(args: Array[String]): Unit = {
		Twitter.initialize()

		val ssc = new StreamingContext("local[*]", "StreamTransformationsExercise", Seconds(1))

		Logger.getRootLogger.setLevel(Level.ERROR)

		val tweets = TwitterUtils.createStream(ssc, None)

		tweets.filter(_.isRetweet)
		   .count
			.print

		ssc.start
		ssc.awaitTermination()
	}
}
