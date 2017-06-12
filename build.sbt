import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "CMSC33520",
      scalaVersion := "2.11.6",
      version      := "0.1.0"
    )),
    name := "sparkmdd",
	libraryDependencies ++= Seq(
	    "com.google.guava" % "guava" % "19.0",
	    "org.apache.spark" %% "spark-core" % "2.1.1" % "provided"
	)
    //libraryDependencies += scalaTest % Test
  )
