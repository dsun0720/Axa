
name := "Axa"
version := "1.0"
scalaVersion := "2.12.14"

val sparkVersion = "3.1.2"

parallelExecution in test := false
test in assembly := {}
assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", xs@_*) => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}


libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.9" % "test"