package StructuredStreaming.solutions

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.window

object WindowOperationsSolution {
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

		val query = records.toDF("id", "user_name", "place", "reply_to_screen_name", "created_at", "length", "first_hashtag")
			.groupBy(window($"created_at", "10 minutes", "30 seconds"), $"reply_to_screen_name").count()
			.writeStream.format("console")
			.queryName("exerciseOutput").start

		query.awaitTermination()
	}
}
