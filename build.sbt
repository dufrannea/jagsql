val scala3Version = "3.0.0"

lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.2.9"

lazy val root = project
  .in(file("."))
  .settings(
    name := "scala3-simple",
    version := "0.1.0",
    scalaVersion := scala3Version,
    libraryDependencies += "org.typelevel" %% "cats-core" % "2.6.1",
    libraryDependencies += "org.typelevel" %% "cats-free" % "2.6.1",
    libraryDependencies += "org.typelevel" %% "cats-parse" % "0.3.4",
    libraryDependencies += "org.typelevel" %% "cats-effect" % "3.1.1",
    libraryDependencies += "co.fs2" %% "fs2-core" % "3.0.4",
    libraryDependencies += ("com.monovore" %% "decline" % "1.3.0").cross(CrossVersion.for3Use2_13),
    libraryDependencies += scalaTest % Test,
    libraryDependencies += "org.scalatest" %% "scalatest-freespec" % "3.2.9" % Test,
    libraryDependencies += "com.novocode" % "junit-interface" % "0.11" % "test"
  )
