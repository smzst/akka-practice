package aia.stream

import java.nio.file.Paths
import java.nio.file.StandardOpenOption.{APPEND, CREATE, WRITE}

import akka.actor.ActorSystem
import akka.stream.scaladsl.{FileIO, RunnableGraph, Sink, Source}
import akka.stream.{ActorMaterializer, IOResult}
import akka.util.ByteString

import scala.concurrent.{ExecutionContextExecutor, Future}

// $ sbt 'run /path/to/inputFile /path/to/outputFile'
object StreamingCopy {
  def main(args: Array[String]): Unit = {
    val inputFile = Paths.get(args(0))
    val outputFile = Paths.get(args(1))

    val source: Source[ByteString, Future[IOResult]] =
      FileIO.fromPath(inputFile)
    val sink: Sink[ByteString, Future[IOResult]] =
      FileIO.toPath(outputFile, Set(CREATE, WRITE, APPEND))
    val runnableGraph: RunnableGraph[Future[IOResult]] =
      source.to(sink)

    implicit val system: ActorSystem = ActorSystem()
    implicit val ex: ExecutionContextExecutor = system.dispatcher
    implicit val materializer: ActorMaterializer = ActorMaterializer()

    runnableGraph.run().foreach { result =>
      println(s"${result.status}, ${result.count} bytes read.")
      system.terminate()
    }
  }
}
