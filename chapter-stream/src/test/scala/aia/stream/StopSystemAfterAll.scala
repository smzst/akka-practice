package aia.stream

import akka.testkit.TestKit
import org.scalatest.{BeforeAndAfterAll, Suite}

// todo: self type について調べる
trait StopSystemAfterAll extends BeforeAndAfterAll {
  this: TestKit with Suite =>
  override protected def afterAll(): Unit = {
    super.afterAll()
    system.terminate()
  }
}
