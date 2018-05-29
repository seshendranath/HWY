name := "ForgetMe"

version := "1.0"

scalaVersion := "2.11.8"

publishMavenStyle := true

organization := "com.homeaway.analyticsengineering"

resolvers += "HA Maven Snapshots" at "http://mvn-repo.wvrgroup.internal:8081/nexus/content/repositories/snapshots"
resolvers += "HA Maven Releases" at "http://mvn-repo.wvrgroup.internal:8081/nexus/content/repositories/releases"
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
	"org.apache.kafka" % "kafka-clients" % "0.11.0.1-cp1",
	"com.homeaway.commons" % "hacommons-kafka-jackson-avro" % "0.7",
	"com.typesafe.play" %% "play-json" % "2.5.12",
	"com.homeaway" % "forgetme-api" % forgetMeAPIVersion,
	"com.homeaway" % "forgetme-client-okhttp" % forgetMeAPIVersion
)


updateOptions := updateOptions.value.withLatestSnapshots(false)

assemblyMergeStrategy in assembly := {
	case PathList("META-INF", _@_ *) => MergeStrategy.discard
	case _ => MergeStrategy.first
}
