package SparkStreamingBasics.exercises

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import util.Twitter

object TransformOperationExercise {

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
		// TODO: add your solution here
	}
}
