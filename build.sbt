ThisBuild / scalaVersion := "2.12.18"

val sparkVersion = "3.5.3"

lazy val root = (project in file("."))
  .settings(
    name := "ProgettoScalable",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
    )
  )
