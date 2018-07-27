name := "ForgetMe"

version := "1.0"

scalaVersion := "2.11.8"

publishMavenStyle := true

organization := "com.homeaway.analyticsengineering"

resolvers += "confluent" at "http://packages.confluent.io/maven/"

crossPaths := false

publishArtifact in(Compile, packageSrc) := true

val forgetMeAPIVersion = "0.0.11"

libraryDependencies ++= Seq(
	"com.typesafe" % "config" % "1.3.1",
	"org.json4s" %% "json4s-jackson" % "3.5.3",
	"com.github.scopt" %% "scopt" % "3.5.0",
	"com.microsoft.sqlserver" % "mssql-jdbc" % "6.2.1.jre8",
	"log4j" % "log4j" % "1.2.17",
	"com.github.nscala-time" %% "nscala-time" % "2.16.0",
	"org.apache.kafka" % "kafka-clients" % "1.1.0",
	"org.apache.kafka" %% "kafka" % "1.1.0",
	"org.apache.spark" %% "spark-streaming-kafka" % "1.6.3",
	"com.typesafe.play" %% "play-json" % "2.5.12"
)


updateOptions := updateOptions.value.withLatestSnapshots(false)

assemblyMergeStrategy in assembly := {
	case PathList("META-INF", _@_ *) => MergeStrategy.discard
	case _ => MergeStrategy.first
}
