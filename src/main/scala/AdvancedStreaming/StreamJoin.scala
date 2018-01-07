package AdvancedStreaming

import com.github.catalystcode.fortis.spark.streaming.reddit.RedditUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import util.Reddit

object StreamJoin {
	def main(args: Array[String]): Unit = {
		val ssc = new StreamingContext("local[*]", "StreamJoin", Seconds(4))

		Logger.getRootLogger.setLevel(Level.ERROR)

		val redditStream = RedditUtils.createPageStream(Reddit.auth, List("sports"), ssc, pollingPeriodInSeconds = 2)
	   	.filter(_.data.subreddit != null)

		val shortStream = redditStream.window(Seconds(20))
			.map(r => (r.data.subreddit.get, r.data.title.get))

		val longStream = redditStream.window(Minutes(2))
			.map(r => (r.data.subreddit.get, 1))
			.reduceByKey(_ + _)

		shortStream.join(longStream)
		   .transform(_.sortBy(_._2._2, ascending = false)) // sort by 2-minute-count of posts in subreddit (topic), descending
			.print

		ssc.start
		ssc.awaitTermination()
	}
}
