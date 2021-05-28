import Dependencies._

ThisBuild / scalaVersion     := "2.13.5"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val cata = (project in file("cata"))
  .settings(
    name := "cata",
    libraryDependencies += "org.typelevel" %% "cats-core" % "2.6.0",
    libraryDependencies += "org.typelevel" %% "cats-free" % "2.6.0",
    libraryDependencies += "org.typelevel" %% "cats-parse" % "0.3.3",
    libraryDependencies += "org.typelevel" %% "cats-effect" % "3.1.0",
    libraryDependencies += scalaTest % Test,
    // libraryDependencies += "com.lihaoyi" %% "mainargs" % "0.2.1",
    libraryDependencies += "com.monovore" %% "decline" % "1.3.0",
    libraryDependencies += "com.lihaoyi" %% "fansi" % "0.2.14",
    assembly / mainClass := Some("duff.Main"),
    wartremoverErrors ++= Nil
  )

lazy val grep = (project in file("grep"))
.settings(
  name := "cata",
  libraryDependencies += "org.typelevel" %% "cats-core" % "2.6.0",
  libraryDependencies += "org.typelevel" %% "cats-free" % "2.6.0",
  libraryDependencies += "org.typelevel" %% "cats-parse" % "0.3.3",
  libraryDependencies += "org.typelevel" %% "cats-effect" % "3.1.0",
  libraryDependencies += scalaTest % Test,
  // libraryDependencies += "com.lihaoyi" %% "mainargs" % "0.2.1",
  libraryDependencies += "com.monovore" %% "decline" % "1.3.0",
  libraryDependencies += "com.lihaoyi" %% "fansi" % "0.2.14",
  assembly / mainClass := Some("duff.Main"),
  wartremoverErrors ++= Warts.unsafe
)


// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
