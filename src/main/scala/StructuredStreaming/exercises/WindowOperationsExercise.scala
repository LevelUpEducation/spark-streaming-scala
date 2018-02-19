package StructuredStreaming.exercises

object WindowOperationsExercise {
	/**
	  * Exercise:
	  *
	  * Use the tweet file source and the window function to determine how many tweets were
	  * created in response to another tweet within a 10 minute window. Update the output
	  * every 30 seconds.
	  *
	  * Bonus requirement: Calculate the percentage of tweet replies in relation to the total
	  * number of tweets within the time window.
	  *
	  * Example Ouptut:
	  *
	  * +--------------------+-----------------+---------+
	  * |              window|replyToScreenName|count(id)|
	  * +--------------------+-----------------+---------+
	  * |[2018-02-17 14:34...|       MarkWarner|        1|
	  * |[2018-02-17 14:28...|      deadlydalal|        1|
	  * |[2018-02-17 14:35...|  SavageYugyeomie|        2|
	  * |[2018-02-17 14:36...|   LiveNBA_Stream|        5|
	  * |[2018-02-17 14:30...|        Woodguy55|        1|
	  * ...
	  * |[2018-02-17 14:31...|         AherysR6|        3|
	  * +--------------------+-----------------+---------+
	  * only showing top 20 rows
	  */
	def main(args: Array[String]): Unit = {
		// TODO: add your solution here
	}
}