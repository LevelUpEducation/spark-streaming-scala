package SparkStreamingBasics.exercises

import com.github.catalystcode.fortis.spark.streaming.reddit.RedditUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import util.Reddit

object ReduceByKeyAndWindowSolution {

	/**
	  * Exercise:
	  *
	  * Use the reduceByKeyAndWindow() function to produce a list of websites (host names) from the URL
	  * of Reddit posts along with the number of posts within a 30 second time window. The list should
	  * get updated every 5 seconds.
	  *
	  * Example Output:
	  *
	  * (www.reddit.com,8)
	  * (hosted.ap.org,1)
	  * (www.newsday.com,1)
	  * (i.redd.it,1)
	  * (www.miamiherald.com,9)
	  * (www.latimes.com,1)
	  * (www.chicagotribune.com,1)
	  * (beta.latimes.com,1)
	  * (www.google.ie,1)
	  * (imgur.com,1)
	  *
	  * @param args
	  */
	def main(args: Array[String]): Unit = {
		val ssc = new StreamingContext("local[*]", "ReduceByKeyAndWindowExercise", Seconds(5))

		Logger.getRootLogger.setLevel(Level.ERROR)

		val stream = RedditUtils.createPageStream(Reddit.auth, List("sports"), ssc, pollingPeriodInSeconds=10)

		stream.filter(_.data.url.isDefined)
			.map(p => p.data.url.get.split("/")(2) -> 1)
			.reduceByKeyAndWindow(_ + _, Seconds(30))
			.print

		ssc.start
		ssc.awaitTermination()
	}
}
