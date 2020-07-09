package aia.stream

import java.nio.file.StandardOpenOption._
import java.nio.file.{Files, Path}

import aia.stream.LogEntityMarshaller.LEM
import akka.Done
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.http.scaladsl.unmarshalling.FromEntityUnmarshaller
import akka.stream.scaladsl.{FileIO, Flow, Keep, Sink, Source}
import akka.stream.{IOResult, Materializer}
import akka.util.ByteString
import spray.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class ContentNegLogsApi(val logsDir: Path,
                        val maxLine: Int,
                        val maxJsonObject: Int)(
  implicit val executionContext: ExecutionContext,
  val materializer: Materializer
) extends EventMarshalling {
  private def logFile(id: String) = logsDir.resolve(id)

  private val outFlow = Flow[Event].map { event =>
    ByteString(event.toJson.compactPrint)
  }

  private def logFileSource(
    logId: String
  ): Source[ByteString, Future[IOResult]] =
    FileIO.fromPath(logFile(logId))
  private def logFileSink(logId: String): Sink[ByteString, Future[IOResult]] =
    FileIO.toPath(logFile(logId), Set(CREATE, WRITE, APPEND))

  def routes: Route = postRoute ~ getRoute ~ deleteRoute

  implicit val unmarshaller: FromEntityUnmarshaller[Source[Event, _]] =
    EventUnmarshaller.create(maxLine, maxJsonObject)
  def postRoute: Route =
    pathPrefix("logs" / Segment) { logId =>
      pathEndOrSingleSlash {
        post {
          entity(as[Source[Event, _]]) { src => // as[T] を使うのに FromEntityUnmarshaller[T] の Unmarshaller が必要
            onComplete(
              src.via(outFlow).toMat(logFileSink(logId))(Keep.right).run()
            ) {
              case Success(IOResult(count, Success(Done))) =>
                complete(StatusCodes.OK -> LogReceipt(logId, count))
              case Success(IOResult(_, Failure(e))) =>
                complete(StatusCodes.BadRequest -> e.getMessage)
              case Failure(e) =>
                complete(
                  StatusCodes.BadRequest -> ParseError(logId, e.getMessage)
                )
            }
          }
        }
      }
    }

  implicit val marshaller: LEM = LogEntityMarshaller.create(maxJsonObject)
  def getRoute: Route =
    pathPrefix("logs" / Segment) { logId =>
      pathEndOrSingleSlash {
        get {
          extractRequest { req =>
            if (Files.exists(logFile(logId))) {
              val src = logFileSource(logId)
              // todo: Marshal(src).toResponseFor(req) が何やってるのかよく分からないので調べる
              complete(Marshal(src).toResponseFor(req))
            } else {
              complete(StatusCodes.NotFound)
            }
          }
        }
      }
    }

  def deleteRoute: Route =
    pathPrefix("logs" / Segment) { logId =>
      pathEndOrSingleSlash {
        delete {
          if (Files.deleteIfExists(logFile(logId))) {
            complete(StatusCodes.OK)
          } else {
            complete(StatusCodes.NotFound)
          }
        }
      }
    }

}
