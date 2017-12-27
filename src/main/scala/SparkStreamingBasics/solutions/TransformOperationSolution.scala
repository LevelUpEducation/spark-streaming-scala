package SparkStreamingBasics.exercises

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import util.Twitter

object TransformOperationSolution {

	/**
	  * Exercise:
	  *
	  * Identify tweets that are replies to other tweets using the getInReplyToScreenName() function.
	  * Convert each tweet into a pair of the screen name the reply is directed at and the tweet's length.
	  * Use the transform function to sort the resulting pairs alphabetically by screen name.
	  *
	  * @param args
	  */
	def main(args: Array[String]): Unit = {
		Twitter.initialize()

		val ssc = new StreamingContext("local[*]", "TransformOperationExercise", Seconds(2))

		Logger.getRootLogger.setLevel(Level.ERROR)

		val tweets = TwitterUtils.createStream(ssc, None)

		tweets.filter(_.getInReplyToScreenName != null)
			.map(tweet => tweet.getInReplyToScreenName -> tweet.getText.length)
			.transform(_.sortByKey())
			.print

		ssc.start
		ssc.awaitTermination()
	}
}
