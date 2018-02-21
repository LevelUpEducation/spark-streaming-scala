package StructuredStreaming.exercises

object WindowOperationExercise {
	/**
	  * Exercise:
	  *
	  * Use the tweet file source and the window function to determine how many tweets were
	  * created in response to another tweet within a 10 minute window. We are also interested
	  * in the length of the first hashtag in each time window. Please list that along with
	  * the response tweet count. Update the output every 30 seconds.
	  * Please use the typed API to filter and map the records in this exercise as much as
	  * possible, in order to catch any issues related to the data types as early as possible.
	  * It may sometimes take a couple minutes until you have any tweet replies show up in
	  * your output.
	  *
	  * Bonus requirement: Calculate the percentage of tweet replies in relation to the total
	  * number of tweets within the time window.
	  *
	  * Example Output:
	  *
	  * +--------------------+-----------------+---------+------------------+
	  * |              window|replyToScreenName|count(id)|max(hashtagLength)|
	  * +--------------------+-----------------+---------+------------------+
	  * |[2018-02-19 19:37...|       oliviaraho|        4|                 0|
	  * |[2018-02-19 19:37...|      BradMossEsq|        1|                 0|
	  * |[2018-02-19 19:37...|           KamVTV|        2|                 0|
	  * |[2018-02-19 19:37...|   TheLaurenVotes|        1|                12|
	  * ...
	  * |[2018-02-19 19:37...|    nickperkins17|        1|                 0|
	  * +--------------------+-----------------+---------+------------------+
	  * only showing top 20 rows
	  */
	def main(args: Array[String]): Unit = {
		// TODO: add your solution here
	}
}