name := "code"

version := "0.1"

scalaVersion := "2.13.3"

libraryDependencies ++= Seq("com.typesafe" % "config" % "1.4.0",
  "ch.qos.logback" % "logback-classic" % "1.3.0-alpha5",
  "org.scalatest" %% "scalatest" % "3.0.8" % "test",
  "org.apache.hadoop" % "hadoop-client" % "3.3.0",
  "org.scala-lang.modules" %% "scala-xml" % "2.0.0-M2"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

