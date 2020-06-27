import akka.http.scaladsl.marshalling.{Marshaller, ToEntityMarshaller}
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.stream.scaladsl.Source
import akka.util.ByteString

object LogEntityMarshaller extends EventMarshalling {

  type LEM = ToEntityMarshaller[Source[ByteString, _]]
  def create(maxJsonObject: Int): LEM = {
    val js = ContentTypes.`application/json`
    val txt = ContentTypes.`text/plain(UTF-8)`

    val jsMarshaller = Marshaller.withFixedContentType(js) {
      src: Source[ByteString, _] =>
        HttpEntity(js, src)
    }

    val txtMarshaller = Marshaller.withFixedContentType(txt) {
      src: Source[ByteString, _] =>
        HttpEntity(txt, toText())
    }
  }

  def toText(src: Source[ByteString, _], maxJsonObject: Int) = {
    src.via(LogJson.jsonToLogFlow)
  }
}
