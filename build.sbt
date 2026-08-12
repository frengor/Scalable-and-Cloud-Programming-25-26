ThisBuild / version := "1.0.0"

ThisBuild / scalaVersion := "2.12.18"

val sparkVersion = "3.5.3"

lazy val root = (project in file("."))
  .settings(
    name := "ScalableCourseProject",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
    )
  )
