package SparkStreamingBasics.exercises

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import util.Twitter

object CountByValueAndWindowSolution {

	/**
	  * Exercise:
	  *
	  * Count the use of languages (two-letter code) in tweets using the countByValueAndWindow() function.
	  * Display the counts from the last 20 seconds every 5 seconds.
	  *
	  * Example Output:
	  *
	  * (pt,61)
	  * (ht,1)
	  * (tl,4)
	  * (eu,1)
	  * (th,14)
	  * (pl,1)
	  * (fr,11)
	  * (ko,25)
	  * (en,225)
	  * (de,1)
	  *
	  * @param args
	  */
	def main(args: Array[String]): Unit = {
		Twitter.initialize()

		val ssc = new StreamingContext("local[*]", "CountByValueAndWindowExercise", Seconds(1))

		Logger.getRootLogger.setLevel(Level.ERROR)

		val tweets = TwitterUtils.createStream(ssc, None)

		ssc.checkpoint("checkpoints")

		tweets
			.map(_.getLang)
			.countByValueAndWindow(Seconds(20), Seconds(5))
			.print

		ssc.start
		ssc.awaitTermination()
	}
}
