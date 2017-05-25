
organization := "com.nitendragautam"

version := "1.0"

scalaVersion := "2.11.8"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

mainClass in assembly := Some("com.nitendragautam.sparkbatchapp.main.Boot")
assemblyJarName in assembly := "sparksbatchapp.jar"
libraryDependencies ++= {
val sparkV     =  "2.0.1"

  Seq(
    "org.apache.spark" % "spark-streaming_2.11" % sparkV % "provided",
    "org.apache.spark" % "spark-core_2.11" % sparkV % "provided",
    "log4j" % "log4j" % "1.2.17"

  )}
