import java.time.ZonedDateTime

case class Event(host: String,
                 service: String,
                 state: State,
                 time: ZonedDateTime,
                 description: String,
                 tag: Option[String] = None,
                 metric: Option[Double] = None)

sealed trait State
case object Critical extends State
case object Error extends State
case object Ok extends State
case object Warning extends State

object State {
  def normalize(str: String): String = str.toLowerCase
  def normalize(state: State): String = normalize(state.toString)

  val ok = normalize(Ok)
  val warning = normalize(Warning)
  val error = normalize(Error)
  val critical = normalize(Critical)

  def unapply(str: String): Option[State] = {
    val normalized = normalize(str)
    if (normalized == normalize(Ok)) Some(Ok)
    else if (normalized == normalize(Warning)) Some(Warning)
    else if (normalized == normalize(Error)) Some(Error)
    else if (normalized == normalize(Critical)) Some(Critical)
    else None
  }
}

case class LogReceipt(logId: String, written: Long)
case class ParseError(logId: String, message: String)
