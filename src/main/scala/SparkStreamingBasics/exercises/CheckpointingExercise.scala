package SparkStreamingBasics.exercises

object CheckpointingExercise {

	/**
	  * Exercise:
	  *
	  * Use Checkpointing and the reduceByKeyAndWindow() function to count
	  * how many Twitter users have the same location. Please sort the results
	  * by the count in descending order with the help of the transform() or
	  * foreachRDD() function.
	  * Use a window duration of twenty seconds and print the output every five
	  * seconds.
	  * To ensure the Checkpointing works, please stop the execution a few
	  * times after it ran for a while and start it again to make sure it picks 
	  * up at the point where it left off. This requires that you obtain the
	  * streaming context using StreamingContext.getOrCreate() using a custom
	  * function as a parameter.
	  *
	  * Example Output:
	  *
	  * (London, England,4)
	  * (France,4)
	  * (Panamá,3)
	  * (México,3)
	  * (United States,3)
	  * (Australia,2)
	  * (Mississippi Mills, Ontario,2)
	  * (Dubai, United Arab Emirates,2)
	  * (Buffalo, NY,2)
	  * ...
	  */
	def main(args: Array[String]): Unit = {
		// TODO: add your solution here
	}
}
