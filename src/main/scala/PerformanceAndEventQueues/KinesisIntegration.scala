package PerformanceAndEventQueues

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Duration, Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.kinesis._
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import org.apache.spark.storage.StorageLevel

object KinesisIntegration {
	def main(args: Array[String]): Unit = {
		val ssc = new StreamingContext("local[*]", "KinesisIntegration", Seconds(2))

		Logger.getRootLogger.setLevel(Level.ERROR)

		val kinesisStream = KinesisInputDStream.builder.streamingContext(ssc)
			.checkpointAppName("App Name")
			.streamName("Stream Name")
			.endpointUrl("kinesis.us-east-1.amazonaws.com")
			.regionName("us-east-1")
			.initialPositionInStream(InitialPositionInStream.LATEST)
			.checkpointInterval(Duration(2000))
			.storageLevel(StorageLevel.MEMORY_AND_DISK_2)
			.build

		// Message format: eventName:userId:userName:jsonEventData
		kinesisStream.filter(_.startsWith("signup:"))
			.map(_.toString.split(":")(1))
			.countByValueAndWindow(Minutes(5), Seconds(30))
			.map(count => s"Received $count signup events in the last 5 minutes")
			.print

		ssc.checkpoint("kinesisIntegration")
		ssc.start
		ssc.awaitTermination()
	}

}
