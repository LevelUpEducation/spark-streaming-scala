package SparkStreamingBasics.solutions

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import util.Twitter

object CheckpointingSolution {
	val checkpointDirectory = "checkpointingSolution"

	def main(args: Array[String]): Unit = {
		// Get StreamingContext from data stored in the checkpoint directory or create a new one
		val ssc = StreamingContext.getOrCreate(checkpointDirectory, functionToCreateContext)

		Logger.getRootLogger.setLevel(Level.ERROR)

		ssc.start
		ssc.awaitTermination()
	}

	private def functionToCreateContext(): StreamingContext = {
		val ssc = new StreamingContext("local[*]", "CheckpointingSolution", Seconds(5))

		Twitter.initialize()
		val tweets = TwitterUtils.createStream(ssc, None)

		tweets.filter(_.getUser.getLocation != null)
			.map(t => (t.getUser.getLocation, 1))
		   .reduceByKeyAndWindow(_ + _, _ - _, Seconds(20), Seconds(5))
			.transform(_.sortBy(_._2, ascending = false))
			.print

		ssc.checkpoint(checkpointDirectory)
		ssc
	}
}
