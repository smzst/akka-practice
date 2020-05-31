import java.nio.charset.StandardCharsets.UTF_8

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.IOResult
import akka.stream.scaladsl._
import akka.util.ByteString
import com.typesafe.config.ConfigFactory
import spray.json._

import scala.concurrent.{ExecutionContextExecutor, Future}

// BIDI は「バイダイ」と読むようだ
object BidiEventFilter extends App with EventMarshalling {
  val config = ConfigFactory.load()
  val maxLine = config.getInt("log-stream-processor.max-line")
  val maxJsonObject = config.getInt("log-stream-processor.max-json-object")

  if (args.length != 5) {
    System.err.println(
      "Provide args: input-format output-format input-file output-file state"
    )
    System.exit(1)
  }

  val inputFile = FileArg.shellExpanded(args(2))
  val outputFile = FileArg.shellExpanded(args(3))
  val filterState = args(4) match {
    case State(state) => state
    case unknown =>
      System.err.println(s"Unknown state $unknown, exiting.")
      System.exit(1)
  }

  val inFlow: Flow[ByteString, Event, NotUsed] =
    if (args(0).toLowerCase == "json") {
      JsonFraming
        .objectScanner(maxJsonObject)
        .map(_.decodeString(UTF_8).parseJson.convertTo[Event])
    } else {
      Framing
        .delimiter(ByteString("\n"), maxLine)
        .map(_.decodeString(UTF_8))
        .map(LogStreamProcessor.parseLineEx)
        .collect { case Some(e) => e }
    }

  val outFlow: Flow[Event, ByteString, NotUsed] =
    if (args(1).toLowerCase == "json") {
      Flow[Event].map(e => ByteString(e.toJson.compactPrint))
    } else {
      Flow[Event].map { e =>
        ByteString(LogStreamProcessor.logLine(e))
      }
    }

  val bidiFlow = BidiFlow.fromFlows(inFlow, outFlow)

  val source: Source[ByteString, Future[IOResult]] =
    FileIO.fromPath(inputFile)
  val sink: Sink[ByteString, Future[IOResult]] =
    FileIO.toPath(outputFile)

  val filter: Flow[Event, Event, NotUsed] =
    Flow[Event].filter(_.state == filterState)

  // note: 入力側 bidiFlow -> filter、出力側 bidiFlow <- filter
  val flow = bidiFlow.join(filter)

  val runnableGraph: RunnableGraph[Future[IOResult]] =
    source.via(flow).toMat(sink)(Keep.right)

  implicit val system: ActorSystem = ActorSystem()
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  runnableGraph.run().foreach { result =>
    println(s"Wrote ${result.count} bytes to '$outputFile'.")
    system.terminate()
  }
}
