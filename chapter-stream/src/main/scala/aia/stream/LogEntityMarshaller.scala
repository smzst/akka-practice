package aia.stream

import akka.http.scaladsl.marshalling.{Marshaller, ToEntityMarshaller}
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.stream.scaladsl.Source
import akka.util.ByteString

object LogEntityMarshaller extends EventMarshalling {
  type LEM = ToEntityMarshaller[Source[ByteString, _]]
  private val js = ContentTypes.`application/json`
  private val txt = ContentTypes.`text/plain(UTF-8)`

  def create(maxJsonObject: Int): LEM = {
    val jsMarshaller = Marshaller.withFixedContentType(js) {
      src: Source[ByteString, _] =>
        HttpEntity(js, src)
    }
    val txtMarshaller = Marshaller.withFixedContentType(txt) {
      src: Source[ByteString, _] =>
        HttpEntity(txt, toText(src, maxJsonObject))
    }

    Marshaller.oneOf(jsMarshaller, txtMarshaller)
  }

  def toText(src: Source[ByteString, _],
             maxJsonObject: Int): Source[ByteString, _] = {
    src.via(LogJson.jsonToLogFlow(maxJsonObject))
  }
}
