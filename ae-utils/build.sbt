lazy val `ae-utils` = project

lazy val root = project.in( file(".") ).aggregate(
  `ae-utils`
).settings(
  packageSrc := file(".")
 )
 .enablePlugins(MinimalBuildPlugin)
 .disablePlugins(AssemblyPlugin)
