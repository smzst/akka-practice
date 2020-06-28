package aia.stream

import spray.json.{DefaultJsonProtocol, JsonFormat}

trait MetricMarshalling extends EventMarshalling with DefaultJsonProtocol {
  implicit val metric: JsonFormat[Metric] = jsonFormat5(Metric)
}
