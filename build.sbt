ThisBuild / scalaVersion     := "2.13.10"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.kitlangton"
ThisBuild / organizationName := "kitlangton"

val zioVersion = "2.0.5"

lazy val root = (project in file("."))
  .settings(
    name := "raft",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % zioVersion,
      "dev.zio" %% "zio-test" % zioVersion % Test,
      "com.lihaoyi" %% "pprint" % "0.8.1"
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )
