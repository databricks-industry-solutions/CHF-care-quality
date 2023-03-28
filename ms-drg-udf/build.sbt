lazy val scala212 = "2.12.9"
lazy val scala211 = "2.11.12"
lazy val sparkVersion = sys.env.getOrElse("SPARK_VERSION", "3.3.0")
name := "ms-drg-udf"
version := "0.1.0"
lazy val drgversion = "39" //version of the DRG Grouper

ThisBuild / scalaVersion := sys.env.getOrElse("SCALA_VERSION", scala212)
ThisBuild / organization := "com.databricks.labs"
ThisBuild / organizationName := "Databricks, Inc."

lazy val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-catalyst" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
).map(_ % Provided)

lazy val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.2.14" 
).map(_ % Test)

lazy val coreDependencies = Seq(
  "com.google.protobuf" % "protobuf-java" % "4.0.0-rc-2",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.5",
)

libraryDependencies ++= sparkDependencies ++ testDependencies ++ coreDependencies
lazy val core = (project in file("."))
  .settings(
    scalacOptions += "-target:jvm-1.8",
  )

Compile / doc / scalacOptions += "-no-java-comments"

assemblyJarName := s"${name.value}-${version.value}." + s"drg-v${drgversion}.jar" 
