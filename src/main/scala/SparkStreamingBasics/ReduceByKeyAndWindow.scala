package SparkStreamingBasics

import com.github.catalystcode.fortis.spark.streaming.reddit.RedditUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import util.Reddit

object ReduceByKeyAndWindow {
	def main(args: Array[String]): Unit = {
		val ssc = new StreamingContext("local[*]", "ReduceByKeyAndWindow", Seconds(5))

		Logger.getRootLogger.setLevel(Level.ERROR)

		val stream = RedditUtils.createPageStream(Reddit.auth, List("sports"), ssc, pollingPeriodInSeconds=10)

		stream.filter(_.data.url.isDefined)
			.map(p => p.data.url.get -> p.data.title.getOrElse("").length)
		   .reduceByKeyAndWindow(_ + _, Seconds(10))
			.print

		ssc.start
		ssc.awaitTermination()
	}
}
