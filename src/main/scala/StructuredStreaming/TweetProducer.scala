package StructuredStreaming

import java.nio.file.{Files, StandardCopyOption}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import util.Twitter

import scala.reflect.io.File

object TweetProducer {
	def main(args: Array[String]): Unit = {
		Twitter.initialize()

		val ssc = new StreamingContext("local[2]", "TweetProducer", Seconds(1))

		Logger.getRootLogger.setLevel(Level.ERROR)

		val tweets = TwitterUtils.createStream(ssc, None)
		val tmpDirectory = "tweetFilesTmp/"
		val directory = "tweetFiles/"

		tweets
		.foreachRDD{rdd =>
			if (rdd.count() > 0) {
				val fileName = "tweets_" + System.currentTimeMillis()
				val tmpPath = new java.io.File(tmpDirectory + fileName)
				val path = new java.io.File(directory + fileName)

				File(tmpPath.toString())
					.writeAll( rdd.filter(_.getLang == "en")
						.map(t => t.getId + "," + t.getUser.getName + "," + (if (t.getPlace == null) "" else t.getPlace.getName)
							+ "," + t.getInReplyToScreenName + "," + t.getCreatedAt.getTime
							+ "," + t.getText.length + "," + t.getHashtagEntities.map(_.getText).headOption.getOrElse(""))
						.collect().mkString("\n") )

				Files.move(tmpPath.toPath, path.toPath, StandardCopyOption.ATOMIC_MOVE)

				/* Maybe replace with something like this:
				val saveFunc = (rdd: RDD[T], time: Time) => {
			      val file = rddToFileName(prefix, suffix, time)
			      rdd.saveAsTextFile(file)
			   }
			   this.foreachRDD(saveFunc, displayInnerRDDOps = false)
				 */

				/*import java.io.PrintStream
				import java.net.ServerSocket
				val server = new ServerSocket(9999)
				val conn = server.accept
				val out = new PrintStream(conn.getOutputStream)
				out.println(rdd.count())*/
			}
		}

		ssc.start
		ssc.awaitTermination()
	}
}
