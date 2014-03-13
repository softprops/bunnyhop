package bunnyhop

import com.rabbitmq.client.{
  AMQP, Channel, DefaultConsumer, Envelope, ShutdownSignalException
}

import scala.util.control.NonFatal
import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.atomic.AtomicReference

/** A Queue is a description of what a consumer may subscribe to.
 *  In most cases a Queue should be bound to an exchange (via bind)
 *  before it can subscribe to messages
 */
case class Queue(
  name: String,
  chan: Chan,
  durable: Boolean = false,
  exclusive: Boolean = false,
  autoDelete: Boolean = false,
  bindings: List[(Exchange, String)] = Nil) {

  chan.underlying.queueDeclare(
    name, durable, exclusive, autoDelete, null)

  def bind(ex: Exchange, routingKey: String = "") = {
    chan.underlying.queueBind(
      name, ex.name, routingKey)
    copy(bindings = (ex, routingKey) :: Nil)
  }

  /** subscribe to incoming messages. shutdown signals not initiated by the application
   *  will trigger a reconnect attempt, rebinding, and re-subscription automatically.
   *  messages will be ack(nowledg)ed after function f completes
   */
  def subscribe(
    f: (Envelope, AMQP.BasicProperties, Array[Byte]) => Unit): Unit = {
    chan.underlying.basicConsume(
      name, false/*auto ack*/,
      new DefaultConsumer(chan.underlying) {
        override def handleDelivery(
          consumerTag: String, envelope: Envelope,
          props: AMQP.BasicProperties, body: Array[Byte]) {
            try f(envelope, props, body) finally {
              chan.ack(envelope.getDeliveryTag, false)
            }
        }
        override def handleShutdownSignal(tag: String, sig: ShutdownSignalException) =
          if (!sig.isInitiatedByApplication) {
            // attempt to reconnect, rebind, & resubscribe
            ((copy() /: bindings) {
              case (q, (ex, routing)) =>
                q.bind(ex, routing)
            }).subscribe(f)
          }
      }) 
  }
}

