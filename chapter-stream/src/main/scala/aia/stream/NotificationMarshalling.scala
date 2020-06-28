package aia.stream

import spray.json.{DefaultJsonProtocol, JsonFormat}

trait NotificationMarshalling
    extends EventMarshalling
    with DefaultJsonProtocol {
  implicit val summary: JsonFormat[Summary] = jsonFormat1(Summary)
}
