name := "kafka-streams-scala-words-count"

version := "0.1"

scalaVersion := "2.13.3"


val kafkaVersion = "2.6.0"
val logback = "1.2.3"
libraryDependencies ++=  Seq(
  "org.apache.kafka" %% "kafka-streams-scala" % kafkaVersion,
  "org.apache.kafka" % "kafka-clients" % kafkaVersion,
  "ch.qos.logback" % "logback-core" % logback,
  "ch.qos.logback" % "logback-classic" % logback,
)