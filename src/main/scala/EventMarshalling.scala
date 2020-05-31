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
            case e: DateTimeParseException =>
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

  implicit val eventFormat: JsonFormat[Event] = jsonFormat7(Event)
  implicit val logIdFormat: JsonFormat[LogReceipt] = jsonFormat2(LogReceipt)
  implicit val errorFormat: JsonFormat[ParseError] = jsonFormat2(ParseError)
}
