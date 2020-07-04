name := "akka-in-action"

version := "1.0"

lazy val stream = project.in(file("chapter-stream"))

parallelExecution in Test := false
