ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "streamingwithscala"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.5.2",
  "org.apache.spark" %% "spark-core" % "3.5.2",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.2",
)