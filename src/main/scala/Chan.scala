package bunnyhop

import com.rabbitmq.client.{
  AMQP, ConfirmListener, Channel, Envelope, ReturnListener, ShutdownSignalException
}

sealed trait Confirm
object Confirm {
  case class Ack(tag: Long, multiple: Boolean) extends Confirm
  case class Nack(tag: Long, multiple: Boolean) extends Confirm
}

/** A Chan provides a means of creating queues
 *  to consume from and exchanges to publish to */
case class Chan(
  underlying: Channel,
  confirmListeners: List[Confirm => Unit] = Nil,
  returnListeners: List[(Int, String, String, String, AMQP.BasicProperties, Array[Byte]) => Unit] = Nil) {

  def onConfirm(f: Confirm => Unit): Chan = {
    underlying.addConfirmListener(new ConfirmListener {
      def handleAck(tag: Long, multiple: Boolean) = f(Confirm.Ack(tag, multiple))
      def handleNack(tag: Long, multiple: Boolean) = f(Confirm.Nack(tag, multiple))
    })
    copy(confirmListeners = f :: confirmListeners)
  }

  def onReturn(f: (Int, String, String, String, AMQP.BasicProperties, Array[Byte]) => Unit): Chan = {
    underlying.addReturnListener(new ReturnListener {
      def handleReturn(
        replyCode: Int, replyText: String,
        exchange: String, routingKey: String, props:AMQP.BasicProperties, body: Array[Byte]) =
          f(replyCode, replyText, exchange, routingKey, props, body)
    })
    copy(returnListeners = f :: returnListeners)
  }

  def isOpen: Boolean = underlying.isOpen

  def closeReason: ShutdownSignalException =
    underlying.getCloseReason

  def queue(
    name: String,
    durable: Boolean = false,
    autoDelete: Boolean = true,
    exclusive: Boolean = false) =
    Queue(name, this,
          durable = durable,
          autoDelete = autoDelete,
          exclusive = exclusive)

  def defaultExchange = Exchange("", this)

  def fanout(exchange: String) = {
    underlying.exchangeDeclare(exchange, "fanout")
    Exchange(exchange, this)
  }

  /** a message sent with a particular routing key will be delivered
   *  to all the queues that are bound with a matching binding key */
  def topic(exchange: String) = {
    underlying.exchangeDeclare(exchange, "topic")
    Exchange(exchange, this)
  }

  /** a message goes to the queues whose binding key exactly
   *  matches the routing key of the message. */
  def direct(exchange: String) = {
    underlying.exchangeDeclare(exchange, "direct")
    Exchange(exchange, this)
  }

  def ack(tag: Long, multiple: Boolean) =
    underlying.basicAck(tag, multiple)

  def abort() = underlying.abort()

  def close() {
    underlying.close()
    underlying.getConnection.close()
  }
}
