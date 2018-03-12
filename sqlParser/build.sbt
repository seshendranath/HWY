name := "sqlParser"

version := "1.0"

scalaVersion := "2.12.2"

libraryDependencies ++= Seq(
  "org.hibernate" % "hibernate-core" % "4.0.1.Final"
  , "log4j" % "log4j" % "1.2.14"
  , "com.github.scopt" %% "scopt" % "3.5.0"
  , "com.typesafe" % "config" % "1.3.1"
  , "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.6"
  , "com.typesafe.play" %% "play-json" % "2.6.0"
)
// logging


assemblyDefaultJarName in assembly := "sqlParser.jar"

