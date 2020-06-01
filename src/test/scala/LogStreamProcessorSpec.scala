import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, StandardOpenOption}
import java.time.ZonedDateTime

import akka.actor.ActorSystem
import akka.stream.IOResult
import akka.stream.scaladsl._
import akka.testkit.TestKit
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class LogStreamProcessorSpec
    extends TestKit(ActorSystem("test-filter"))
    with AnyWordSpecLike
    with Matchers
    with StopSystemAfterAll {

  val lines: String =
    "my-host-1  | web-app | ok       | 2015-08-12T12:12:00.127Z | 5 tickets sold to RHCP.||\n" +
      "my-host-2  | web-app | ok       | 2015-08-12T12:12:01.127Z | 3 tickets sold to RHCP.| | \n" +
      "my-host-3  | web-app | ok       | 2015-08-12T12:12:02.127Z | 1 tickets sold to RHCP.| | \n" +
      "my-host-3  | web-app | error    | 2015-08-12T12:12:03.127Z | exception occurred...| | \n"

  "A log stream processor" must {
    "be able to read a log file and parse events" in {
      val path = Files.createTempFile("logs", ".txt")

      val bytes = lines.getBytes(UTF_8)
      Files.write(path, bytes, StandardOpenOption.APPEND)

      import LogStreamProcessor._

      val source: Source[String, Future[IOResult]] = logLines(path)

      val eventsSource: Source[Event, Future[IOResult]] =
        errors(parseLogEvents(source))

      // RunnableGraph.run と Source.run は全然別物
      // notes: run は Sink.ignore に接続しストリームの要素は破棄（`Done` を返す）。runWith は Sink のマテリアライズされた値を返す（今回の例では Vector(Event(...))）。
      val events: Future[Seq[Event]] = eventsSource.runWith(Sink.seq[Event])

      Await.result(events, Duration("10 seconds")) must be(
        Vector(
          Event(
            "my-host-3",
            "web-app",
            Error,
            ZonedDateTime.parse("2015-08-12T12:12:03.127Z"),
            "exception occurred..."
          )
        )
      )

    }

    "be able to read it's own output" in {
      val path = Files.createTempFile("logs", ".json")
      val json =
        """
      [
      {
        "host": "my-host-1",
        "service": "web-app",
        "state": "ok",
        "time": "2015-08-12T12:12:00.127Z",
        "description": "5 tickets sold to RHCP."
      },
      {
        "host": "my-host-2",
        "service": "web-app",
        "state": "ok",
        "time": "2015-08-12T12:12:01.127Z",
        "description": "3 tickets sold to RHCP."
      },
      {
        "host": "my-host-3",
        "service": "web-app",
        "state": "ok",
        "time": "2015-08-12T12:12:02.127Z",
        "description": "1 tickets sold to RHCP."
      },
      {
        "host": "my-host-3",
        "service": "web-app",
        "state": "error",
        "time": "2015-08-12T12:12:03.127Z",
        "description": "exception occurred..."
      }
      ]
      """

      val bytes = json.getBytes(UTF_8)
      Files.write(path, bytes, StandardOpenOption.APPEND)

      import LogStreamProcessor._
      val source = jsonText(path)

      val results = errors(parseJsonEvents(source)).runWith(Sink.seq[Event])

      Await.result(results, Duration("10 seconds")) must be(
        Vector(
          Event(
            "my-host-3",
            "web-app",
            Error,
            ZonedDateTime.parse("2015-08-12T12:12:03.127Z"),
            "exception occurred..."
          )
        )
      )
    }
  }
}
