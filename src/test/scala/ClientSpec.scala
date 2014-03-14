package bunnyhop

import org.scalatest.FunSpec

import java.util.concurrent.{ CountDownLatch, TimeUnit }

class ClientSpec extends FunSpec {
  describe("bunnyhop") {

    it ("should support basic pub/sub") {
      val chan  = Connector().channel
      val q     = chan.queue("example", autoDelete = true)
      val ex    = chan.defaultExchange
      val latch = new CountDownLatch(1)
      q.subscribe {
        case (_, _, _) => latch.countDown()
      }
      ex.publish("hello".getBytes, routingKey = q.name)
      latch.await(1, TimeUnit.SECONDS)
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
      latch.await(1, TimeUnit.SECONDS)
      chan.close()
    }

    it ("should support direct pub/sub") {
      val chan  = Connector().channel
      val ex    = chan.direct("dir")
      val latch = new CountDownLatch(2)
      chan.queue("", autoDelete = true).bind(ex, routingKey = "a")
        .subscribe {
          case (_, _, _) => latch.countDown()
        }
      chan.queue("", autoDelete = true).bind(ex, routingKey = "b")
        .subscribe {
          case (_, _, _) => latch.countDown()
        }
      val msg = "test".getBytes
      ex.publish(msg, routingKey = "a")
      ex.publish(msg, routingKey = "b")
      latch.await(1, TimeUnit.SECONDS)
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
      latch.await(1, TimeUnit.SECONDS)
      chan.close()
    }

    it ("should support header pub/sub") {
      val chan  = Connector().channel
      val ex    = chan.headers("headers")
      val latch = new CountDownLatch(3)
      chan.queue("").bind(ex, arguments = Map(
        "os" -> "linux", "cores" -> 8, "x-match" -> "all"))
        .subscribe {
          case (_, _, _) => latch.countDown()
        }
      chan.queue("").bind(ex, arguments = Map(
        "os" -> "osx", "cores" -> 8, "x-match" -> "any"))
        .subscribe {
          case (_, _, _) => latch.countDown()
        }
      val msg = "test".getBytes
      ex.publish(msg, headers = Map("os" -> "linux", "cores" -> 8))
      ex.publish(msg, headers = Map("os" -> "osx", "cores" -> 8))
      ex.publish(msg, headers = Map("os" -> "linux", "cores" -> 4))
      latch.await(1, TimeUnit.SECONDS)
      chan.close()
    }

    it ("should support multiple routing key bindings") {
      val chan = Connector().channel
      val ex   = chan.topic("multi")
      val latch = new CountDownLatch(2)
      chan.queue("bar").bind(ex, routingKey = "a").bind(ex, routingKey = "b")
        .subscribe {
          case (_, _, _) => latch.countDown()
        }
      val msg = "test".getBytes
      ex.publish(msg, routingKey = "a")
      ex.publish(msg, routingKey = "b")
      latch.await(1, TimeUnit.SECONDS)
      chan.close()
    }

  }
}
