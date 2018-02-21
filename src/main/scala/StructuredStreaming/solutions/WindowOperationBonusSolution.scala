package StructuredStreaming.solutions

import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{count, max, sum, window}

object WindowOperationBonusSolution {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession
			.builder
			.appName("WindowOperationsBonusSolution")
			.master("local[*]")
			.getOrCreate()

		import spark.implicits._

		Logger.getRootLogger.setLevel(Level.ERROR)

		spark.read.json("tweetFiles").printSchema()

		val schema = spark.read
			.format("csv")
			.option("header", value = false)
			.option("inferSchema", value = true)
			.load("tweetFiles")
			.schema

		val records = spark.readStream
			.schema(schema)
			.format("csv")
			.option("header", value = false)
			.load("tweetFiles")

		case class TweetData(id: BigInt, userName: String, place: String, replyToScreenName: String,
		                     createdAt: String, textLength: BigInt, firstHashtag: String)

		implicit val tweetDataEncoder = org.apache.spark.sql.Encoders.kryo[TweetData]

		val query = records.
			as[(BigInt, String, String, String, String, BigInt, String)].
			map(r => TweetData(r._1, r._2, r._3, r._4, r._5, r._6, r._7)).
			filter(r => r.createdAt != null && r.createdAt != "null").
			map(t => (t.replyToScreenName, new Timestamp(t.createdAt.toLong), t.id,
						if (t.firstHashtag == null) 0 else t.firstHashtag.length,
						if (t.replyToScreenName == null) 0 else 1)).
			toDF("replyToScreenName", "createdAt", "id", "hashtagLength", "reply").
			withWatermark("createdAt", "3 minutes").
			groupBy(window($"createdAt", "10 minutes", "30 seconds"), $"replyToScreenName").
			agg(count("id").as("count"), sum("reply").as("replies"), max("hashtagLength")).
			select($"window", $"replyToScreenName", $"count", $"replies" / $"count" * 100 as "RepliesPercentage",
				$"max(hashtagLength)").
			writeStream.format("console").
			queryName("exerciseOutput").start

		query.awaitTermination()
	}

}
