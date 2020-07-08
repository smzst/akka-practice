package aia.stream

import java.nio.file.StandardOpenOption.{APPEND, CREATE, WRITE}
import java.nio.file.{Files, Path}

import akka.http.scaladsl.common.{
  EntityStreamingSupport,
  JsonEntityStreamingSupport
}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.unmarshalling.FromEntityUnmarshaller
import akka.stream.scaladsl.{
  Broadcast,
  FileIO,
  Flow,
  Keep,
  MergePreferred,
  Source
}
import akka.stream.{IOResult, Materializer, OverflowStrategy}
import akka.util.ByteString
import akka.{Done, NotUsed}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class LogStreamProcessorApi(val logsDir: Path,
                            val notificationDir: Path,
                            val metricsDir: Path,
                            val maxLine: Int,
                            val maxJsObject: Int)(
  implicit val executionContext: ExecutionContext,
  val materializer: Materializer
) extends EventMarshalling {
  def logFile(id: String) = logsDir.resolve(id)

  val notificationSink = FileIO.toPath(
    notificationDir.resolve("notifications.json"),
    Set(CREATE, WRITE, APPEND)
  )
  val metricsSink = FileIO.toPath(
    metricsDir.resolve("metrics.json"),
    Set(CREATE, WRITE, APPEND)
  )

  import akka.stream.scaladsl.GraphDSL
  import akka.stream.{FlowShape, Graph}

  type FlowLike = Graph[FlowShape[Event, ByteString], NotUsed]

  def processEvents(logId: String): FlowLike = {
    val jsFlow = LogJson.jsonOutFlow
    val notifyOutFlow = LogJson.notifyOutFlow
    val metricOutFlow = LogJson.metricOutFlow

    Flow.fromGraph(GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val nrWarnings = 100
      val nrErrors = 10
      val archBufSize = 100000
      val warnBufSize = 100
      val errBufSize = 1000
      val errDuration = 10 seconds
      val warnDuration = 1 minute

      val toMetric = Flow[Event].collect {
        case Event(_, service, _, time, _, Some(tag), Some(metric)) =>
          Metric(service, time, metric, tag)
      }

      // note: expand を使うことで、利用できるようになっている要素よりも多くの要素をコンシューマーが要求したときに出力に情報を追加できる。
      //  この例だと、処理速度が LogStreamProcessor < 監視サービス の場合に drift に 0 より大きな値を詰めて返す
      val recordDrift: Flow[Metric, Metric, NotUsed] = Flow[Metric].expand {
        metric =>
          Iterator.from(0).map(d => metric.copy(drift = d))
      }

      val bcast = builder.add(Broadcast[Event](5))
      val wbcast = builder.add(Broadcast[Event](2))
      val ebcast = builder.add(Broadcast[Event](2))
      val cbcast = builder.add(Broadcast[Event](2))
      val okcast = builder.add(Broadcast[Event](2))

      val mergeNotify = builder.add(MergePreferred[Summary](2))
      val archive = builder.add(jsFlow)

      val toNot = Flow[Event].map(e => Summary(Vector(e)))

      val ok = Flow[Event].filter(_.state == Ok)
      val warning = Flow[Event].filter(_.state == Warning)
      val error = Flow[Event].filter(_.state == Error)
      val critical = Flow[Event].filter(_.state == Critical)

      def rollup(nr: Int, duration: FiniteDuration) =
        Flow[Event]
          .groupedWithin(nr, duration)
          .map(events => Summary(events.toVector))

      val rollupErr = rollup(nrErrors, errDuration)
      val rollupWarn = rollup(nrWarnings, warnDuration)

      val archBuf = Flow[Event].buffer(archBufSize, OverflowStrategy.fail)
      val warnBuf = Flow[Event].buffer(warnBufSize, OverflowStrategy.dropHead)
      val errBuf = Flow[Event].buffer(errBufSize, OverflowStrategy.backpressure)
      val metricBuf = Flow[Event].buffer(errBufSize, OverflowStrategy.dropHead)

      // format: off
      bcast ~> archBuf  ~> archive.in
      bcast ~> ok       ~> okcast
      bcast ~> warning  ~> wbcast
      bcast ~> error    ~> ebcast
      bcast ~> critical ~> cbcast

      okcast ~> jsFlow ~> logFileSink(logId, Ok)
      okcast ~> metricBuf ~> toMetric ~> recordDrift ~> metricOutFlow ~> metricsSink

      cbcast ~> jsFlow ~> logFileSink(logId, Critical)
      cbcast ~> toNot ~> mergeNotify.preferred

      ebcast ~> jsFlow ~> logFileSink(logId, Error)
      ebcast ~> errBuf ~> rollupErr ~> mergeNotify.in(0)

      wbcast ~> jsFlow ~> logFileSink(logId, Warning)
      wbcast ~> warnBuf ~> rollupWarn ~> mergeNotify.in(1)
      
      mergeNotify ~> notifyOutFlow ~> notificationSink
      // format: on

      FlowShape(bcast.in, archive.out)
    })
  }

  def logFileSource(logId: String, state: State) =
    FileIO.fromPath(logStateFile(logId, state))
  def logFileSink(logId: String, state: State) =
    FileIO.toPath(logStateFile(logId, state), Set(CREATE, WRITE, APPEND))
  def logStateFile(logId: String, state: State) =
    logFile(s"$logId-${State.normalize(state)}")

  import akka.stream.SourceShape
  import akka.stream.scaladsl.{GraphDSL, Merge}

  def mergeNotOk(logId: String): Source[ByteString, NotUsed] = {
    val warning =
      logFileSource(logId, Warning).via(LogJson.jsonFramed(maxJsObject))
    val error = logFileSource(logId, Error).via(LogJson.jsonFramed(maxJsObject))
    val critical =
      logFileSource(logId, Critical).via(LogJson.jsonFramed(maxJsObject))

    Source.fromGraph(GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val warningShape = builder.add(warning)
      val errorShape = builder.add(error)
      val criticalShape = builder.add(critical)
      val merge = builder.add(Merge[ByteString](3))

      // format: off
      warningShape  ~> merge
      errorShape    ~> merge
      criticalShape ~> merge
      // format: on

      SourceShape(merge.out)
    })
  }

  def logFileSource(logId: String) = FileIO.fromPath(logFile(logId))
  def archiveSink(logId: String) =
    FileIO.toPath(logFile(logId), Set(CREATE, WRITE, APPEND))
  def logFileSink(logId: String) =
    FileIO.toPath(logFile(logId), Set(CREATE, WRITE, APPEND))

  def route: Route = ???

  implicit val unmarshaller: FromEntityUnmarshaller[Source[Event, _]] =
    EventUnmarshaller.create(maxLine, maxJsObject)
  implicit val jsonStreamingSupport: JsonEntityStreamingSupport =
    EntityStreamingSupport.json()

  def postRoute =
    pathPrefix("logs" / Segment) { logId =>
      pathEndOrSingleSlash {
        post {
          entity(asSourceOf[Event]) { src =>
            onComplete(
              src
                .via(processEvents(logId))
                .toMat(archiveSink(logId))(Keep.right)
                .run()
            ) {
              case Success(IOResult(count, Success(Done))) =>
                complete(StatusCodes.OK -> LogReceipt(logId, count))
              case Success(IOResult(_, Failure(e))) =>
                complete(
                  StatusCodes.BadRequest -> ParseError(logId, e.getMessage)
                )
              case Failure(e) =>
                complete(
                  StatusCodes.BadRequest -> ParseError(logId, e.getMessage)
                )
            }
          }
        }
      }
    }

  implicit val marshaller = LogEntityMarshaller.create(maxJsObject)

  def getRoute =
    pathPrefix("logs" / Segment) { logId =>
      pathEndOrSingleSlash {
        get {
          extractRequest { req =>
            if (Files.exists(logFile(logId))) {
              val src = logFileSource(logId)
              complete(Marshal(src).toResponseFor(req))
            } else {
              complete(StatusCodes.NotFound)
            }
          }
        }
      }
    }

  val StateSegment = Segment.flatMap {
    case State(state) => Some(state)
    case _            => None
  }

  def getLogStateRoute =
    pathPrefix("logs" / Segment / StateSegment) { (logId, state) =>
      pathEndOrSingleSlash {
        get {
          extractRequest { req =>
            if (Files.exists(logStateFile(logId, state))) {
              val src = logFileSource(logId, state)
              complete(Marshal(src).toResponseFor(req))
            } else {
              complete(StatusCodes.NotFound)
            }
          }
        }
      }
    }

  import akka.stream.scaladsl.Merge

  def mergeSource[E](source: Vector[Source[E, _]]): Option[Source[E, _]] = {
    if (source.isEmpty) None
    if (source.size == 1) Some(source(0))
    else {
      Some(Source.combine(source(0), source(1), source.drop(2): _*)(Merge(_)))
    }
  }
}
