package SparkStreamingBasics

import com.github.catalystcode.fortis.spark.streaming.reddit.RedditUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import util.Reddit

object ForeachRDD {
	def main(args: Array[String]): Unit = {
		val ssc = new StreamingContext("local[*]", "ForeachRDD", Seconds(10))

		Logger.getRootLogger.setLevel(Level.ERROR)

		val stream = RedditUtils.createPageStream(Reddit.auth, List("sports"), ssc, pollingPeriodInSeconds=10)

		var count = 1
		stream.foreachRDD { rdd =>
				if (rdd.count() > 0) {
					rdd.map(p => "'" + p.data.title.get + "','" + p.data.url.get + "'")
					   	.saveAsTextFile(s"output$count")
					println(s"Saved CSV file to directory output$count")
					count += 1
				}
			}

		ssc.start
		ssc.awaitTermination()
	}
}
