package aia.stream

import java.time.ZonedDateTime

case class Metric(service: String,
                  time: ZonedDateTime,
                  metric: Double,
                  tag: String,
                  drift: Int = 0)
