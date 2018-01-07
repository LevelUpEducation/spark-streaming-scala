package AdvancedStreaming.solutions

import com.github.catalystcode.fortis.spark.streaming.reddit.RedditUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import util.Reddit

object UpdateStateByKeySolution {
	def main(args: Array[String]): Unit = {
		val ssc = new StreamingContext("local[*]", "UpdateStateByKeyExercise", Seconds(5))

		ssc.checkpoint("checkpoints")

		Logger.getRootLogger.setLevel(Level.ERROR)

		val titleLengths = RedditUtils.createPageStream(Reddit.auth, List("sports"), ssc, pollingPeriodInSeconds = 2)
			.filter(_.data.title.isDefined)
			.map(r => (r.data.title.get.split(" ").length + " words", 1))

		titleLengths//.updateStateByKey(updateFunction)
			.foreachRDD{rdd =>
				println(s"Most common word count in Reddit titles (${rdd.count}):")
				rdd.sortBy(_._2, ascending = false)
					.foreach(r => println(r._1 + ": " + r._2))
			}

		ssc.start
		ssc.awaitTermination()
	}

	private def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
		val count = runningCount.getOrElse(0) + newValues.sum
		Some(count)
	}
}
