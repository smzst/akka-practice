import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Path
import java.time.ZonedDateTime

import akka.stream.IOResult
import akka.stream.scaladsl.{FileIO, Flow, Framing, JsonFraming, Source}
import akka.util.ByteString
import spray.json._

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

object LogStreamProcessor extends EventMarshalling {
  def logLines(path: Path): Source[String, Future[IOResult]] =
    delimitedText(FileIO.fromPath(path), 1024 * 1024)

  def convertToString[T](source: Source[ByteString, T]): Source[String, T] =
    source.map(_.decodeString(UTF_8))

  def delimitedText[T](source: Source[ByteString, T],
                       maxLine: Int): Source[String, T] =
    convertToString(source.via(Framing.delimiter(ByteString("\n"), maxLine)))

  // note: `collect` は case にマッチした結果だけのコレクションにする (※ `Traversable[A].collect` の説明)
  // filter と map を合わせたものだが、型で判別できるので Option 型に対しては collect 使った方が便利そう
  def parseLogEvents[T](source: Source[String, T]): Source[Event, T] =
    source.map(parseLineEx).collect { case Some(e) => e }

  def errors[T](source: Source[Event, T]): Source[Event, T] =
    source.filter(_.state == Error)

  // note: `groupedWithin` よくわかってない
  // finiteDuration だけ待って、最大 n 要素分溜めて Source とするものっぽい
  def rollup[T](source: Source[Event, T],
                predicate: Event => Boolean,
                nrEvent: Int,
                duration: FiniteDuration): Source[Seq[Event], T] =
    source.filter(predicate).groupedWithin(nrEvent, duration)

//  def groupByHost[T](source: Source[Event, T]) = {
//    source.groupBy(10, e => (e.host, e.service))
//  }

  def convertToJsonBytes[T](
    flow: Flow[Seq[Event], Seq[Event], T]
  ): Flow[Seq[Event], ByteString, T] =
    flow.map(events => ByteString(events.toJson.compactPrint))

  // Path から JSON 文字列の Source を返す
  def jsonText(path: Path): Source[String, Future[IOResult]] =
    jsonText(FileIO.fromPath(path), 1024 * 1024)

  // Source から JSON 文字列の Source を返す
  def jsonText[T](source: Source[ByteString, T],
                  maxObject: Int): Source[String, T] =
    convertToString(source.via(JsonFraming.objectScanner(maxObject)))

  def parseJsonEvents[T](source: Source[String, T]): Source[Event, T] =
    source.map(_.parseJson.convertTo[Event])

  def parseLineEx(line: String): Option[Event] = {
    if (!line.isEmpty) {
      line.split("\\|") match {
        case Array(host, service, state, time, description, tag, metric) =>
          // note: `trim` は前後の空白を除去した文字列を返す
          val t = tag.trim
          val m = metric.trim
          Some(
            Event(
              host.trim,
              service.trim,
              state.trim match {
                case State(s) => s
                case _        => throw new Exception(s"Unexpected state: $line")
              },
              ZonedDateTime.parse(time.trim),
              description.trim,
              if (t.nonEmpty) Some(t) else None,
              if (m.nonEmpty) Some(t.toDouble) else None
            )
          )
        case Array(host, service, state, time, description) =>
          Some(Event(host.trim, service.trim, state.trim match {
            case State(s) => s
            case _        => throw new Exception(s"Unexpected state: $line")
          }, ZonedDateTime.parse(time.trim), description.trim))
        case x =>
          throw new LogParseException(s"Failed on line: $line")
      }
    } else None
  }

  def logLine(event: Event): String = {
    s"""${event.host} | ${event.service} | ${State.normalize(event.state)} | ${event.time.toString} | ${event.description} | ${if (event.tag.nonEmpty)
      "|" + event.tag.get
    else "|"} ${if (event.metric.nonEmpty) "|" + event.metric.get else "|"}\n"""
  }

  case class LogParseException(message: String) extends Exception(message)
}
