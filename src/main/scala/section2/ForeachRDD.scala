package section2

import com.github.catalystcode.fortis.spark.streaming.reddit.RedditUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import util.Reddit

object ForeachRDD {
	def main(args: Array[String]): Unit = {
		val ssc = new StreamingContext("local[*]", "ForeachRDD", Seconds(10))

		Logger.getRootLogger.setLevel(Level.ERROR)

		val stream = RedditUtils.createPageStream(Reddit.auth, List("sports"), ssc, pollingPeriodInSeconds=10)

		var count = 1
		stream
			.map(p => (p.data.title.get, p.data.url.get))
			.foreachRDD { rdd =>
				if (rdd.count() > 0) {
					val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
					import spark.implicits._

					rdd.toDF.write.csv("output" + count)
					println("saved CSV file to output directory #" + count)
					count += 1
				}
			}

		ssc.start
		ssc.awaitTermination()
	}
}
