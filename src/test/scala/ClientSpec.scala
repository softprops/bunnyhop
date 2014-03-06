package bunnyhop

import org.scalatest.FunSpec

import java.util.concurrent.CountDownLatch

class ClientSpec extends FunSpec {
  describe("client") {

    it ("should support basic pub/sub") {
      val chan  = Connector().channel
      val q     = chan.queue("example", autoDelete = true)
      val ex    = chan.defaultExchange
      val latch = new CountDownLatch(1)
      q.subscribe {
        case (_, _, _) => latch.countDown()
      }
      ex.publish("hello".getBytes, routingKey = q.name)
      latch.await()
      chan.close()
    }

    it ("should support fanout pub/sub") {
      val chan  = Connector().channel
      val ex    = chan.fanout("logs")
      val latch = new CountDownLatch(2)
      chan.queue("app", autoDelete = true).bind(ex)
        .subscribe {
          case (_, _, _) => latch.countDown()
        }
      chan.queue("api", autoDelete = true).bind(ex)
        .subscribe {
          case (_, _, _) => latch.countDown()
        }
      ex.publish("test".getBytes)
      latch.await()
      chan.close()
    }

    it ("should support topic pub/sub") {
      val chan  = Connector().channel
      val ex    = chan.topic("tops")
      val latch = new CountDownLatch(3)
      chan.queue("foo").bind(ex, routingKey = "foo.bar")
        .subscribe {
          case (_, _, _) => latch.countDown()
        }
      chan.queue("bar").bind(ex, routingKey = "foo.#")
        .subscribe {
          case (_, _, _) => latch.countDown()
        }
      chan.queue("baz").bind(ex, routingKey = "#.bar")
        .subscribe {
          case (_, _, _) => latch.countDown()
        }
      ex.publish("test".getBytes, routingKey = "foo.bar")
      latch.await()
      chan.close()
    }
  }
}
