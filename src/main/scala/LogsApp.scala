import java.nio.file.{FileSystems, Files}

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Supervision}
import com.typesafe.config.ConfigFactory

import scala.concurrent.{ExecutionContextExecutor, Future}

object LogsApp extends App {

  val config = ConfigFactory.load()
  val host = config.getString("http.host")
  val port = config.getString("http.port")

  val logsDir = {
    val dir = config.getString("log-stream-processor.logs-dir")
    // todo: FileSystems.getDefault とは
    Files.createDirectories(FileSystems.getDefault.getPath(dir))
  }
  val maxLine = config.getInt("log-stream-processor.max-line")

  implicit val system: ActorSystem = ActorSystem()
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  // どのエラーが起きても失敗させる（マテリアライズされた値は、例外を含む失敗した Future になる）
  val decider: Supervision.Decider = {
    case _: LogStreamProcessor.LogParseException => Supervision.Stop
    case _                                       => Supervision.Stop
  }

  // fixme: duplicated
  implicit val materializer = ActorMaterializer(
    ActorMaterializerSettings(system).withSupervisionStrategy(decider)
  )

  val api = new LogsApi(logsDir, maxLine).routes

  val bindingFuture: Future[ServerBinding] =
    Http().bindAndHandle(api, host, port)

  val log = Logging(system.eventStream, "logs")

  bindingFuture
    .map { serverBinding =>
      log.info(s"Bound to ${serverBinding.localAddress} ")
    }
    .recover {
      case ex: Exception =>
        log.error(ex, "Failed to bind {}:{}!", host, port)
        system.terminate()
    }
}
