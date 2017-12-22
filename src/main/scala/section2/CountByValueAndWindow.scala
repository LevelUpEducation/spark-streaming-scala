package section2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import util.Twitter

object CountByValueAndWindow {
	def main(args: Array[String]): Unit = {
		Twitter.initialize()

		val ssc = new StreamingContext("local[*]", "CountByValueAndWindow", Seconds(1))

		Logger.getRootLogger.setLevel(Level.ERROR)

		val tweets = TwitterUtils.createStream(ssc, None)

		tweets.map(_.getHashtagEntities.head.getText)
			.countByValueAndWindow(Seconds(5), Seconds(3))
			.print

		ssc.start
		ssc.awaitTermination()
	}

}
