package AdvancedStreaming

import com.github.catalystcode.fortis.spark.streaming.reddit.RedditUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import util.Reddit

object StreamDatasetJoin {
	def main(args: Array[String]): Unit = {
		val ssc = new StreamingContext("local[*]", "StreamDatasetJoin", Seconds(4))

		Logger.getRootLogger.setLevel(Level.ERROR)

		val redditStream = RedditUtils.createPageStream(Reddit.auth, List("sports"), ssc, pollingPeriodInSeconds = 2)

		val countTexts = ssc.sparkContext.parallelize(Seq(1 -> "One", 2 -> "Two", 3 -> "Three", 4 -> "Four or more"))

		redditStream
			.filter(_.data.subreddit != null)
			.window(Minutes(1), Seconds(12))
			.map(r => (r.data.subreddit.get, 1))
			.transform( _.reduceByKey(_ + _)
					.map(r => if (r._2 > 3) (4, r._1) else r.swap)
					.join(countTexts)
					.sortByKey(ascending = false)
					.map(_._2)
			)
	   	.print

		ssc.start
		ssc.awaitTermination()
	}
}
