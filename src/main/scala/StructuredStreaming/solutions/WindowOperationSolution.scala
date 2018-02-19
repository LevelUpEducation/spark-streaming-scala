package StructuredStreaming.solutions

import java.sql.Timestamp
import java.util.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.expressions.scalalang.typed
import org.apache.spark.sql.functions._

object WindowOperationSolution {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession
			.builder
			.appName("WindowOperationsExercise")
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

		val query: Dataset[TweetData] = records.
			//printSchema()
			as[(BigInt, String, String, String, String, BigInt, String)].
			map(r => TweetData(r._1, r._2, r._3, r._4, r._5, r._6, r._7))
			//as[TweetData]

		val query2 = query.
			//toDF("id", "userName", "place", "replyToScreenName", "createdAt", "length", "firstHashtag").
			filter(_.replyToScreenName != null).
			//printSchema()
			map(t => (t.replyToScreenName, new Timestamp(t.createdAt.toLong), t.id,
						if (t.firstHashtag == null) 0 else t.firstHashtag.length)).
			toDF("replyToScreenName", "createdAt", "id", "hashtagLength").
			withWatermark("createdAt", "3 minutes").
			groupBy(window($"createdAt", "10 minutes", "30 seconds"), $"replyToScreenName").
			agg(count("id")/*, max("hashtagLength")*/).
			//orderBy($"count(id)".desc).
			writeStream.format("console").
			queryName("exerciseOutput").start

		query2.awaitTermination()
	}
}
