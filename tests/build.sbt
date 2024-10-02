import sbt.Keys._


ThisBuild / version := "0.1.0"

ThisBuild / scalaVersion := "2.12.19"


lazy val root = (project in file("."))
  .settings(
    name := "dpi_garda_consumer"
  )
  .settings(SparkSubmit.settings)



val jacksonVersion = "2.14.2"

val sparkVersion = "3.0.1"

resolvers ++= Seq(
)

scalacOptions += "-Ypartial-unification"

libraryDependencies ++= Seq(

  "com.typesafe" % "config" % "1.4.3",

  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-hive" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-streaming" % sparkVersion % Provided,

  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.kafka" % "kafka-clients" % sparkVersion,
  "org.apache.spark" %% "spark-avro" % sparkVersion,

  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5" % Test,
  "org.scalatest" %% "scalatest" % "3.2.18" % Test,
  "org.scalatest" %% "scalatest-funsuite" % "3.2.18" % Test,

)


assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
