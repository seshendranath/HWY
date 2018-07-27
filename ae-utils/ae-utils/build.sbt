name := "ae-utils"

organization := "com.homeaway.analyticsengineering"

version := "0.1.0"

scalaVersion := "2.11.8"

// https://mvnrepository.com/artifact/log4j/log4j
libraryDependencies += "log4j" % "log4j" % "1.2.17"

// https://mvnrepository.com/artifact/commons-codec/commons-codec
libraryDependencies += "commons-codec" % "commons-codec" % "1.9"

// https://mvnrepository.com/artifact/org.springframework.boot/spring-boot
libraryDependencies += "org.springframework.boot" % "spring-boot" % "1.5.6.RELEASE"

// https://mvnrepository.com/artifact/com.datastax.cassandra/cassandra-driver-core
libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "3.3.0"

// https://mvnrepository.com/artifact/org.eclipse.jgit/org.eclipse.jgit
libraryDependencies += "org.eclipse.jgit" % "org.eclipse.jgit" % "4.9.0.201710071750-r"

// https://mvnrepository.com/artifact/org.mariadb.jdbc/mariadb-java-client-jre7
libraryDependencies += "org.mariadb.jdbc" % "mariadb-java-client-jre7" % "1.6.1"

libraryDependencies += "com.typesafe" % "config" % "1.3.1"

libraryDependencies += "com.amazonaws" % "aws-java-sdk-s3" % "1.11.224"

// https://mvnrepository.com/artifact/org.apache.commons/commons-email
libraryDependencies += "org.apache.commons" % "commons-email" % "1.4"

// https://mvnrepository.com/artifact/com.github.nscala-time/nscala-time
libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.16.0"


val sparkVersion = "2.3.0"

val sparkDependencyScope = "provided"

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % sparkVersion % sparkDependencyScope,
	"org.apache.spark" %% "spark-sql" % sparkVersion % sparkDependencyScope,
	"org.apache.spark" %% "spark-hive" % sparkVersion % sparkDependencyScope,
	"org.apache.spark" %% "spark-yarn" % sparkVersion % sparkDependencyScope
)

// testDeps
libraryDependencies ++= Seq(
	"org.scalatest" %% "scalatest" % "3.0.4" % "test"
)


// Assembly settings
assemblyMergeStrategy in assembly := {
	case PathList("META-INF", xs@_*) => MergeStrategy.discard
	case x =>
		val oldStrategy = (assemblyMergeStrategy in assembly).value
		oldStrategy(x)
}
assemblyJarName in assembly := {
	s"${name.value}-${version.value}.jar"
}
artifactName in assembly := {
	(sVersion: ScalaVersion, module: ModuleID, artifact: Artifact) =>
		s"${name.value}-${version.value}.jar"
}

// Generate the sources jar when the `assembly` task is run (with the correct name)
artifactName in packageSrc := {
	(sVersion: ScalaVersion, module: ModuleID, artifact: Artifact) =>
		s"${name.value}-${version.value}-sources.jar"
}

assembly := assembly.dependsOn(packageSrc.in(Compile)).value

// Exclude the scala libs from the jar
// We do not want to force people to use scala 2.11.8 in all projects using this jar
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
