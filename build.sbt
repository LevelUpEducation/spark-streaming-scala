name := "spark-streaming-scala"

version := "1.0.0"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming" % "2.2.0",
	"org.apache.spark" %% "spark-sql" % "2.2.0",
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.2.0",
  "com.github.catalystcode" %% "streaming-reddit" % "0.0.1"
)