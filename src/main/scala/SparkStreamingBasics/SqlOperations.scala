package SparkStreamingBasics

import com.github.catalystcode.fortis.spark.streaming.reddit.RedditUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import util.Reddit

object SqlOperations {
	def main(args: Array[String]): Unit = {
		val ssc = new StreamingContext("local[*]", "SqlOperations", Seconds(10))

		Logger.getRootLogger.setLevel(Level.ERROR)

		val stream = RedditUtils.createPageStream(Reddit.auth, List("sports"), ssc, pollingPeriodInSeconds=10)

		stream
			.map(p => (p.data.title.get, p.data.subreddit, p.data.created.get.longValue()))
			.foreachRDD { rdd =>
				val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
				import spark.implicits._

				// Convert RDD to DataFrame
				val df = rdd.toDF("title", "topic", "created")

				// Create a temporary view
				df.createOrReplaceTempView("reddit")

				spark.sql("select title, topic, from_unixtime(created) AS created from reddit").show()
			}

		ssc.start
		ssc.awaitTermination()
	}
}
