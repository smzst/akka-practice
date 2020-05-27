import java.nio.file.Paths
import java.nio.file.StandardOpenOption.{APPEND, CREATE, WRITE}

import akka.actor.ActorSystem
import akka.stream.IOResult
import akka.stream.scaladsl.{FileIO, RunnableGraph, Sink, Source}
import akka.util.ByteString

import scala.concurrent.{ExecutionContextExecutor, Future}

// $ sbt 'run /path/to/inputFile /path/to/outputFile'
object Main {
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
    // 2.6.0 以降より非推奨。なくても問題ない。
    // implicit val materializer = ActorMaterializer()

    runnableGraph.run().foreach { result =>
      println(s"${result.status}, ${result.count} bytes read.")
      system.terminate()
    }
  }
}
