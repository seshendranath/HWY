name := "IntegratedMarketing_Load"

version := "1.0-SNAPSHOT"

scalaVersion := "2.11.8"

publishMavenStyle := true

organization := "com.homeaway.analyticsengineering"

publishTo := {
	val nexus = "http://mvn-repo:8081/nexus/"
	if (isSnapshot.value)
		Some("snapshots" at nexus + "content/repositories/snapshots")
	else
		Some("releases" at nexus + "content/repositories/releases")
}


resolvers += "HA Maven Snapshots" at "http://mvn-repo:8081/nexus/content/repositories/snapshots"
resolvers += "HA Maven Releases" at "http://mvn-repo:8081/nexus/content/repositories/releases"

crossPaths := false

publishArtifact in(Compile, packageSrc) := true

val sparkVersion = "2.2.0"

val sparkDependencyScope = "provided"


libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % sparkVersion % sparkDependencyScope,
	"org.apache.spark" %% "spark-sql" % sparkVersion % sparkDependencyScope,
	"org.apache.spark" %% "spark-hive" % sparkVersion % sparkDependencyScope,
	"org.apache.spark" %% "spark-yarn" % sparkVersion % sparkDependencyScope,
	"com.github.scopt" %% "scopt" % "3.5.0",
	"com.typesafe" % "config" % "1.3.1",
	"com.microsoft.sqlserver" % "mssql-jdbc" % "6.2.1.jre8",
	"com.github.nscala-time" %% "nscala-time" % "2.16.0",
	"org.apache.hadoop" % "hadoop-distcp" % "2.8.2" % sparkDependencyScope
	//	"org.apache.hadoop" % "hadoop-tools" % "1.2.1" % sparkDependencyScope
	//	"org.apache.hadoop" % "hadoop-core" % "1.2.1",
	//	"org.apache.hadoop" % "hadoop-common" % "2.8.2"
)


libraryDependencies += (
	"com.homeaway.analyticsengineering" % "ae-utils" % "latest.release"
	)


// https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-s3
libraryDependencies += "com.amazonaws" % "aws-java-sdk-s3" % "1.11.224"


updateOptions := updateOptions.value.withLatestSnapshots(false)

assemblyMergeStrategy in assembly := {
	case PathList("META-INF", _@_ *) => MergeStrategy.discard
	case _ => MergeStrategy.first
}
