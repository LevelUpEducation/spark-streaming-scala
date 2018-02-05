package StructuredStreaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object StructcuredStreaming {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession
			.builder
			.appName("StructuredStreaming")
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

		val lines = spark.readStream
		   .schema(schema)
			.format("csv")
			.option("header", value = false)
	   	.load("tweetFiles")
		/* .format("socket")
			.option("host", "localhost")
			.option("port", 9999)
			.load()*/

		// Split the lines into words
		//val words = lines.as[String].flatMap(_.split(" "))

		val query = lines.toDF("id", "user_name", "place", "reply_to_screen_name", "created_at", "length", "first_hashtag")
		   	.select("user_name", "length", "first_hashtag")
		   	.where("first_hashtag is not null")
				.writeStream.format("console").queryName("someOutput").start
		/*.groupBy("value").count() //.foreach(r => println(r.getInt(0)))
			.writeStream
			.outputMode("complete")
			.format("console")
			.start()*/

		query.awaitTermination()
	}
}
