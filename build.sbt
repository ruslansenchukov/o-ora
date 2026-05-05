import sbtassembly.AssemblyPlugin.autoImport._
import java.nio.file.{Files, Paths}

ThisBuild / organization := "com.company"
ThisBuild / version := "0.1.2-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.19"

val sparkVersion = "3.5.2"
val scalaTestVersion = "3.2.19"
val testcontainersVersion = "1.20.4"
val jnaVersion = "5.14.0"
val propagatedIntegrationProps = Seq("oracle11.it.enabled", "oracle11.it.image").flatMap { key =>
  sys.props.get(key).map(value => s"-D$key=$value")
}
val isColimaDocker = {
  val dockerHostIndicatesColima = sys.env.get("DOCKER_HOST").exists(_.contains(".colima"))
  val colimaSocketExists = {
    val home = sys.props.getOrElse("user.home", "")
    if (home.nonEmpty) Files.exists(Paths.get(home, ".colima", "default", "docker.sock")) else false
  }
  dockerHostIndicatesColima || colimaSocketExists
}
val integrationEnvOverrides =
  if (isColimaDocker && !sys.env.contains("TESTCONTAINERS_RYUK_DISABLED")) {
    Map("TESTCONTAINERS_RYUK_DISABLED" -> "true")
  } else {
    Map.empty[String, String]
  }
val sparkJpmsOptions = Seq(
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
)

lazy val root = (project in file("."))
  .configs(IntegrationTest)
  .settings(Defaults.itSettings)
  .settings(
    name := "spark-oracle11",
    javacOptions ++= Seq("--release", "11"),
    scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-encoding", "UTF-8"),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
      "net.java.dev.jna" % "jna" % jnaVersion,
      "net.java.dev.jna" % "jna-platform" % jnaVersion,
      "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
      "org.scalatest" %% "scalatest" % scalaTestVersion % IntegrationTest,
      "org.testcontainers" % "testcontainers" % testcontainersVersion % IntegrationTest,
      "org.testcontainers" % "oracle-xe" % testcontainersVersion % IntegrationTest
    ),
    Test / fork := true,
    Test / parallelExecution := false,
    Test / javaOptions ++= sparkJpmsOptions,
    IntegrationTest / fork := true,
    IntegrationTest / parallelExecution := false,
    IntegrationTest / javaOptions ++= sparkJpmsOptions ++ propagatedIntegrationProps,
    IntegrationTest / envVars ++= integrationEnvOverrides,
    assembly / test := {},
    assembly / assemblyJarName := s"${name.value}-${version.value}-assembly.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _ @ _*) => MergeStrategy.concat
      case PathList("META-INF", _ @ _*)               => MergeStrategy.discard
      case path                                        => (assembly / assemblyMergeStrategy).value(path)
    }
  )
