package SparkStreamingBasics.exercises

import com.github.catalystcode.fortis.spark.streaming.reddit.RedditUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import util.Reddit

object SqlOperationsSolution {

	/**
	  * Exercise:
	  *
	  * Use SQL operations to count the number of posts in a topic / subreddit within a 20 second time frame.
	  *
	  * Example Output:
	  *
	  * +-----------------+--------+
	  * |            topic|   posts|
	  * +-----------------+--------+
	  * |         WAPOauto|       1|
	  * |        Braincels|       1|
	  * |     USATODAYauto|       1|
	  * |          LazyMan|       1|
	  * |             cars|       1|
	  * |      REUTERSauto|       1|
	  * |       NYPOSTauto|       1|
	  * |           sydney|       1|
	  * |ImagesOfMinnesota|       1|
	  * |    AutoNewspaper|      11|
	  * +-----------------+--------+
	  *
	  * @param args
	  */
	def main(args: Array[String]): Unit = {
		val ssc = new StreamingContext("local[*]", "SqlOperationsExercise", Seconds(20))

		Logger.getRootLogger.setLevel(Level.ERROR)

		val stream = RedditUtils.createPageStream(Reddit.auth, List("sports"), ssc, pollingPeriodInSeconds=10)

		stream
			.map(_.data.subreddit)
			.foreachRDD { rdd =>
				val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
				import spark.implicits._

				// Convert RDD to DataFrame
				val df = rdd.toDF("topic")

				// Create a temporary view
				df.createOrReplaceTempView("reddit")

				spark.sql("select topic, count(*) AS posts from reddit group by topic").show()
			}

		ssc.start
		ssc.awaitTermination()
	}
}
