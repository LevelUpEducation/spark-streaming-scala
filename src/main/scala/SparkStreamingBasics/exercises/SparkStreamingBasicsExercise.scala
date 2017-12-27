package SparkStreamingBasics.exercises

object SparkStreamingBasicsExercise {

	/**
	  * Exercise:
	  *
	  * For this exercise we want to evaluate tweets that have a GeoLocation and a Place.
	  * We want to calculate the average latitude and longitude values from the GeoLocations
	  * and only print places which are northeast of the average point.
	  *
	  * If a place occurs multiple times in the result within the same time window it should
	  * not be repeated but listed with a count > 1, along with the counts for the other
	  * places. The output should be updated every 10 seconds and show results from the last
	  * 5 minutes.
	  *
	  * Spark Streaming functions you might want to use:
	  * countByValueAndWindow, filter, foreachRDD, map, transform, window
	  *
	  * There may be more than one way to solve these requirements.
	  *
	  * Example Output:
	  *
	  * Filtered tweet locations in the northeast of average latitude of 31.84934005333333 and average longitude of 34.29420653111111:
	  * (千葉 浦安市,1)
	  * (東京 練馬区,5)
	  * (千葉 芝山町,22)
	  * (Saidapet, India,2)
	  * (岡山 北区,1)
	  * (長野 塩尻市,1)
	  * (日本,1)
	  *
	  * @param args
	  */
	def main(args: Array[String]): Unit = {
		// TODO: add your solution here
	}
}
