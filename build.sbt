name := "akka-practice"

version := "0.1"

scalaVersion := "2.13.2"

libraryDependencies ++= {
  val akkaVersion = "2.6.5"
  val akkaHttpVersion = "10.1.12"
  Seq(
    "com.typesafe.akka" %% "akka-stream" % akkaVersion,
    "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
    "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
    "org.scalatest" %% "scalatest" % "3.1.2" % "test",
  )
}
