package aia.stream

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.scaladsl.{
  BidiFlow,
  FileIO,
  Flow,
  Framing,
  Keep,
  Sink,
  Source
}
import akka.stream.{IOResult, Materializer}
import akka.util.ByteString
import akka.{Done, NotUsed}
import spray.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class LogsApi(val logsDir: Path, val maxLine: Int)(
  implicit val executionContext: ExecutionContext,
  val materializer: Materializer
) extends EventMarshalling {
  // Path.resolve は、Path に対して渡された引数で解決する
  def logFile(id: String): Path = logsDir.resolve(id)

  val inFlow: Flow[ByteString, Event, NotUsed] = Framing
    .delimiter(ByteString("\n"), maxLine)
    .map(_.decodeString(UTF_8))
    .map(LogStreamProcessor.parseLineEx)
    .collect { case Some(e) => e }
  val outFlow: Flow[Event, ByteString, NotUsed] = Flow[Event].map { event =>
    ByteString(event.toJson.compactPrint)
  }
  val bidiFlow = BidiFlow.fromFlows(inFlow, outFlow)

  import java.nio.file.StandardOpenOption._

  val logToJsonFlow = bidiFlow.join(Flow[Event])

  // note: FileIO.fromPath で Source が、FileIO.toPath で Sink が定義できる
  def logFileSink(logId: String): Sink[ByteString, Future[IOResult]] =
    FileIO.toPath(logFile(logId), Set(CREATE, WRITE, APPEND))
  def logFileSource(logId: String): Source[ByteString, Future[IOResult]] =
    FileIO.fromPath(logFile(logId))

  def routes: Route = postRoute ~ getRoute ~ deleteRoute

  def postRoute: Route =
    pathPrefix("logs" / Segment) { logId => // Segment が logId に相当
      pathEndOrSingleSlash {
        post {
          entity(as[HttpEntity]) { entity =>
            onComplete(
              entity.dataBytes
                .via(logToJsonFlow)
                .toMat(logFileSink(logId))(Keep.right)
                .run()
            ) {
              // > complete() のとこ、ToResponseMarshallable 型を要求してくる… -> 解決済
              // note: complete() の implicit は、JsonFormat[A] ではなく RootJsonFormat[A] である必要があるので注意。
              //  akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._ の import も必要
              case Success(IOResult(count, Success(Done))) =>
                complete(StatusCodes.OK -> LogReceipt(logId, count))
              case Success(IOResult(_, Failure(e))) =>
                complete(
                  StatusCodes.BadRequest -> ParseError(logId, e.getMessage)
                )
              case Failure(e) =>
                complete(
                  StatusCodes.BadRequest -> ParseError(logId, e.getMessage)
                )
            }
          }
        }
      }
    }

  def getRoute: Route =
    pathPrefix("logs" / Segment) { logId =>
      pathEndOrSingleSlash {
        get {
          if (Files.exists(logFile(logId))) {
            val src = logFileSource(logId)
            complete(HttpEntity(ContentTypes.`application/json`, src))
          } else {
            complete(StatusCodes.NotFound)
          }
        }
      }
    }

  def deleteRoute: Route =
    pathPrefix("logs" / Segment) { logId =>
      pathEndOrSingleSlash {
        delete {
          if (Files.deleteIfExists(logFile(logId))) complete(StatusCodes.OK)
          else complete(StatusCodes.InternalServerError)
        }
      }
    }
}
