package SparkStreamingBasics.exercises

import com.github.catalystcode.fortis.spark.streaming.reddit.RedditUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import util.Reddit

object ForeachRDDSolution {

	/**
	  * Exercise:
	  *
	  * Use the foreachRDD function to print a URL for each Reddit post that comes into the stream along with the title
	  * of the post. Use an arrow "->" to separate the two data elements in the output. New entries should show up in the
	  * output every four seconds as they arrive.
	  *
	  * Example Output:
	  *
	  * https://www.reddit.com/r/CharlotteHornets/comments/7lsr4j/post_game_thread_the_charlotte_hornets_1221/ -> [Post Game Thread] The Charlotte Hornets (12-21) defeat the Milwaukee Bucks (17-14), 111-106
	  * http://hosted.ap.org/dynamic/stories/F/FBC_ARMED_FORCES_BOWL_NJOL-?SITE=AP&amp;SECTION=HOME&amp;TEMPLATE=DEFAULT -> [Sports] - 2-point gamble, late TD help Army beat San Diego State 42-35
	  * https://www.reddit.com/r/SSBM/comments/7lsqt4/fantasy_melee_2018_signups_info/ -> Fantasy Melee 2018 - Signups &amp; Info
	  * https://www.newsday.com/sports/hockey/rangers/rangers-kevin-shattenkirk-1.15577736 -> [Sports] - Alain Vigneault wants more urgency from Kevin Shattenkirk
	  * https://www.reddit.com/r/bobbleheads/comments/7lsq8k/looking_for_possible_trades/ -> Looking for possible trades?
	  * https://nypost.com/2017/12/23/advice-for-browns-from-qb-who-knows-the-lasting-pain-of-0-16/ -> [Sports] - Advice for Browns from QB who knows the lasting pain of 0-16
	  * https://www.reddit.com/r/chicago/comments/7lsq2m/vikings_sports_bar_in_the_loop/ -> Vikings sports bar in the loop?
	  *
	  * @param args
	  */
	def main(args: Array[String]): Unit = {
		val ssc = new StreamingContext("local[*]", "ForeachRDD", Seconds(4))

		Logger.getRootLogger.setLevel(Level.ERROR)

		val stream = RedditUtils.createPageStream(Reddit.auth, List("sports"), ssc, pollingPeriodInSeconds=2)

		stream.foreachRDD { rdd =>
				rdd.foreach(r => println(r.data.url.get + " -> " + r.data.title.get))
			}

		ssc.start
		ssc.awaitTermination()
	}
}
