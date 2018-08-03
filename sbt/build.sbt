name := "SpendTaxonomyClassification"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.0.0"

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % sparkVersion % "provided",
	"org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
	"org.apache.spark" %% "spark-mllib-local" % sparkVersion % "provided",
	"org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
	"org.apache.spark" %% "spark-hive" % sparkVersion % "provided"
  )

test in assembly := {}

assemblyJarName in assembly := "EagleStreaming-assembly-1.0.jar"
