package SparkStreamingBasics.exercises

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import util.Twitter

object WindowOperationSolution {

	/**
	  * Exercise:
	  *
	  * Print the total count of characters used in tweets during a 9 second window and print
	  * the resulting count every 3 seconds.
	  *
	  * @param args
	  */
	def main(args: Array[String]): Unit = {
		Twitter.initialize()

		val ssc = new StreamingContext("local[*]", "WindowOperationExercise", Seconds(3))

		Logger.getRootLogger.setLevel(Level.ERROR)

		val tweets = TwitterUtils.createStream(ssc, None)

		tweets
			.map(_.getText.length)
			.window(Seconds(9))
			.reduce(_ + _)
			.print

		ssc.start
		ssc.awaitTermination()
	}

}
