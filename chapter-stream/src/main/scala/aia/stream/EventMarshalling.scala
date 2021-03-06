package aia.stream

import java.time.ZonedDateTime
import java.time.format.{DateTimeFormatter, DateTimeParseException}

import spray.json._

// note: marshalling: 受け取り側の仕様に合った形式に変換してメッセージバッファに詰め込むプロセス
trait EventMarshalling extends DefaultJsonProtocol {
  implicit val dateTimeFormat: JsonFormat[ZonedDateTime] =
    new JsonFormat[ZonedDateTime] {
      override def write(dateTime: ZonedDateTime): JsValue =
        JsString(dateTime.format(DateTimeFormatter.ISO_INSTANT))

      override def read(value: JsValue): ZonedDateTime = value match {
        case JsString(str) =>
          try {
            ZonedDateTime.parse(str)
          } catch {
            case _: DateTimeParseException =>
              val message = s"Could not deserialize $str to ZonedDateTime."
              deserializationError(message)
          }
        case js =>
          val message = s"Could not deserialize $js to ZonedDateTime."
          deserializationError(message)
      }
    }

  implicit val stateFormat: JsonFormat[State] = new JsonFormat[State] {
    override def write(state: State): JsValue =
      JsString(State.normalize(state))

    override def read(value: JsValue): State = value match {
      case JsString("ok")       => Ok
      case JsString("warning")  => Warning
      case JsString("error")    => Error
      case JsString("critical") => Critical
      case js =>
        val message = s"Could not deserialize $js to State."
        deserializationError(message)
    }
  }

  // JsonFormat[A] ではなく RootJsonFormat[A] の必要がある（LogsApi.postRoute のコメント参照）
  implicit val eventFormat: RootJsonFormat[Event] = jsonFormat7(Event)
  implicit val logIdFormat: RootJsonFormat[LogReceipt] = jsonFormat2(LogReceipt)
  implicit val errorFormat: RootJsonFormat[ParseError] = jsonFormat2(ParseError)
}
