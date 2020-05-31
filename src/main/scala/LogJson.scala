import java.nio.charset.StandardCharsets.UTF_8

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Framing, JsonFraming}
import akka.util.ByteString
import spray.json._

object LogJson
    extends EventMarshalling
    with NotificationMarshalling
    with MetricMarshalling {

  def textInFlow(maxLine: Int): Flow[ByteString, Event, NotUsed] = {
    Framing
      .delimiter(ByteString("\n"), maxLine)
      .map(_.decodeString(UTF_8))
      .map(LogStreamProcessor.parseLineEx)
      .collect { case Some(e) => e }
  }

  // note: JsonFraming.objectScanner は Json オブジェクトの配列をオブジェクト単位に区切るもの
  def jsonInFlow(maxJsonObject: Int): Flow[ByteString, Event, NotUsed] = {
    JsonFraming
      .objectScanner(maxJsonObject)
      .map(_.decodeString(UTF_8).parseJson.convertTo[Event])
  }

  def jsonFramed(maxJsonObject: Int): Flow[ByteString, ByteString, NotUsed] =
    JsonFraming.objectScanner(maxJsonObject)

  // note: JsValue.compactPrint は、余計な空白を除いた Json を生成するもの
  val jsonOutFlow: Flow[Event, ByteString, NotUsed] = Flow[Event].map { e =>
    ByteString(e.toJson.compactPrint)
  }

  val notifyOutFlow: Flow[Summary, ByteString, NotUsed] = Flow[Summary].map {
    s =>
      ByteString(s.toJson.compactPrint)
  }

  val metricOutFlow: Flow[Metric, ByteString, NotUsed] = Flow[Metric].map { m =>
    ByteString(m.toJson.compactPrint)
  }

  val textOutFlow: Flow[Event, ByteString, NotUsed] = Flow[Event].map { e =>
    ByteString(LogStreamProcessor.logLine(e))
  }
//
//  def logToJson(maxLine: Int) = {
//
//  }
}
