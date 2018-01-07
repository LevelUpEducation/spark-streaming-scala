package AdvancedStreaming.solutions

import com.github.catalystcode.fortis.spark.streaming.reddit.RedditUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import util.Reddit

object AccumulatorsSolution {
	def main(args: Array[String]): Unit = {
		val ssc = new StreamingContext("local[*]", "AccumulatorsExercise", Seconds(7))

		Logger.getRootLogger.setLevel(Level.ERROR)

		val accum = ssc.sparkContext.longAccumulator("Exercise Accumulator")

		val posts = RedditUtils.createPageStream(Reddit.auth, List("world"), ssc, pollingPeriodInSeconds = 2)

		posts.filter(_.data.title.isDefined)
			.map(_ => accum.add(1))
			.foreachRDD{rdd =>
				rdd.collect() // necessary to ensure the code in our map function above is actually executed
				println("Total count of Reddit posts which have a title: " + accum.sum)
			}

		ssc.start
		ssc.awaitTermination()
	}
}
