package section1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object PrintTwitterStream {
  def main(args: Array[String]): Unit = {
    initializeTwitter()

    val ssc = new StreamingContext("local[*]", "PrintTwitterStream", Seconds(1))

    Logger.getRootLogger.setLevel(Level.ERROR)

    val tweets = TwitterUtils.createStream(ssc, None)

    tweets.map(status => status.getText).print

    ssc.start
    ssc.awaitTermination()
  }

  def initializeTwitter(): Unit = {
    System.setProperty("twitter4j.oauth.consumerKey", sys.env("TW_CON_KEY"))
    System.setProperty("twitter4j.oauth.consumerSecret", sys.env("TW_CON_SECRET"))
    System.setProperty("twitter4j.oauth.accessToken", sys.env("TW_TOKEN"))
    System.setProperty("twitter4j.oauth.accessTokenSecret", sys.env("TW_TOKEN_SECRET"))
  }
}