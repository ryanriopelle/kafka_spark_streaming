name := "EagleStreaming"
version := "1.0"

scalaVersion := "2.11.8"
val sparkVersion = "2.0.0"
//scalaHome := Some(file("/usr/hdp/2.6.4.0-91/spark2"))


//resolvers += 
//"Artifactory" at "https://oneartifactory.verizon.com/artifactory/CV9V_ONEDIGITALMOBILE-SBT-Virtual/"


libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % sparkVersion % "provided",
	"org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
	"org.apache.spark" %% "spark-mllib-local" % sparkVersion % "provided",
	"org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
	"org.apache.spark" %% "spark-hive" % sparkVersion % "provided"
  )

test in assembly := {}

assemblyJarName in assembly := "EagleStreaming-assembly-1.0.jar"
