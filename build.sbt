val scala3Version = "3.1.0"

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
    libraryDependencies += "com.monovore" %% "decline" % "2.1.0",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += "org.scalatest" %% "scalatest-freespec" % "3.2.9" % Test,
    libraryDependencies += "com.novocode" % "junit-interface" % "0.11" % "test",
    libraryDependencies += "co.fs2" % "fs2-core_3" % "3.0.4",
    libraryDependencies += "co.fs2" % "fs2-io_3" % "3.0.4",
    test / envVars := Map("SCALACTIC_FILL_FILE_PATHNAMES" -> "yes"),
    scalacOptions ++= Seq(
      "-deprecation", // Emit warning and location for usages of deprecated APIs.
      "-feature", // Emit warning and location for usages of features that should be imported explicitly.
      "-language:existentials", // Existential types (besides wildcard types) can be written and inferred
      "-language:higherKinds", // Allow higher-kinded types
      "-unchecked", // Enable additional warnings where generated code depends on assumptions.
      "-Xfatal-warnings" // Fail the compilation if there are any warnings.
    ),
    assembly / mainClass := Some("duff.jagsql.Main")
  )
