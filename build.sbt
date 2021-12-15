lazy val commonSettings = Seq(
  version := "0.1",
  scalaVersion := "2.13.6",
  resolvers += "Sonatype Public" at "https://oss.sonatype.org/content/groups/public/",
  resolvers += Resolver.jcenterRepo,
  libraryDependencies ++= Seq(
    "org.scodec" %% "scodec-bits" % "1.1.29",
    "org.scodec" %% "scodec-core" % "1.11.9",
    "org.typelevel" %% "cats-core" % "2.7.0",
    "org.scalacheck" %% "scalacheck" % "1.15.4" % Test,
    "org.scalatest" %% "scalatest" % "3.2.9" % Test,
    "org.apache.avro" % "avro" % "1.11.0" % Test,
  )
)

lazy val core = (project in file("core")).
  settings(
    commonSettings,
    name := "savro-core"
  )

scalaVersion := "2.13.6"
