package aia.stream

import akka.NotUsed
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.Unmarshaller._
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshaller}
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Source}
import akka.util.ByteString

import scala.concurrent.{ExecutionContext, Future}

object EventUnmarshaller extends EventMarshalling {
  val supported: Set[ContentTypeRange] = Set[ContentTypeRange](
    ContentTypes.`text/plain(UTF-8)`,
    ContentTypes.`application/json`
  )

  def create(maxLine: Int,
             maxJsonObject: Int): FromEntityUnmarshaller[Source[Event, _]] = {
    // Unmarshaller トレイトを継承した無名クラスをインスタンス化して、apply メソッドを override してる
    new Unmarshaller[HttpEntity, Source[Event, _]] {
      def apply(entity: HttpEntity)(
        implicit ec: ExecutionContext,
        materializer: Materializer
      ): Future[Source[Event, _]] = {
        val future: Future[Flow[ByteString, Event, NotUsed]] =
          entity.contentType match {
            case ContentTypes.`text/plain(UTF-8)` =>
              Future.successful(LogJson.textInFlow(maxLine))
            case ContentTypes.`application/json` =>
              Future.successful(LogJson.jsonInFlow(maxJsonObject))
            case other =>
              Future.failed(new UnsupportedContentTypeException(supported))
          }
        // note: HttpEntity.dataBytes は、データを読み取るための Source を返す
        future.map(flow => entity.dataBytes.via(flow))
      }
      // note: forContentTypes は指定した ContentType のみ受け入れるように書き換える
    }.forContentTypes(supported.toList: _*)
  }
}
