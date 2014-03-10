package bunnyhop

import com.rabbitmq.client.{
  AMQP, BlockedListener, ConfirmListener, ConnectionFactory, Connection,
  Consumer, Channel, DefaultConsumer, Envelope, ReturnListener,
  ShutdownListener, ShutdownSignalException
}

import scala.util.control.NonFatal
import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.atomic.AtomicReference

/** An Exchange is a description of what a publisher may publish to */
case class Exchange(name: String, chan: Chan) {
  def publish(
    payload: Array[Byte],
    routingKey: String = "",
    properties: AMQP.BasicProperties = null) =
      chan.underlying.basicPublish(
        name, routingKey, properties, payload)
}

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
  binding: Option[(Exchange, String)] = None) {

  chan.underlying.queueDeclare(
    name, durable, exclusive, autoDelete, null)

  def bind(ex: Exchange, routingKey: String = "") = {
    chan.underlying.queueBind(
      name, ex.name, routingKey)
    copy(binding = Some(ex, routingKey))
  }

  /** subscribe to incoming messages. shutdown signals not initiated by the application
   *  will trigger a reconnect attempt, rebinding, and resubscription automatically.
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
            val fresh = copy()
            binding.map { case (ex, routing) => fresh.bind(ex, routing) }
                   .getOrElse(fresh).subscribe(f)
          }
      }) 
  }
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
  def direct(exchanging: String) = {
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

case class Credentials(username: String, password: String)

case class Connector(
  host: String = "localhost",
  port: Int = 5672,
  uri: Option[String] = None,
  vhost: Option[String] = None,
  connectionTimeout: Option[FiniteDuration] = None,
  requestHeartbeat: Option[FiniteDuration] = None,
  maxChannels: Option[Int] = None,
  maxFrameSize: Option[Int] = None,
  credentials: Option[Credentials] = None,
  shutdownHandlers: List[ShutdownSignalException => Unit] = Nil,
  blockHandlers: List[String => Unit] = Nil,
  unblockHandlers: List[() => Unit] = Nil) {

  private[this] val connectionRef = new AtomicReference[Connection]()

  private[this] lazy val factory = new ConnectionFactory() {
    uri.map(setUri(_)).getOrElse {
      setHost(host)
      setPort(port)
    }
    vhost.foreach(setVirtualHost(_))
    connectionTimeout.foreach(to => setRequestedHeartbeat(to.toSeconds.toInt))
    requestHeartbeat.foreach(hb => setRequestedHeartbeat(hb.toSeconds.toInt))
    maxChannels.foreach(setRequestedChannelMax(_))
    maxFrameSize.foreach(setRequestedFrameMax(_))
    credentials.foreach {
      case Credentials(user, pass) =>
        setUsername(user)
        setPassword(pass)
    }
  }

  def onShutdown(f: ShutdownSignalException => Unit) =
   copy(shutdownHandlers = f :: shutdownHandlers)

  def onBlocked(f: String => Unit) =
    copy(blockHandlers = f :: blockHandlers)

  def onUnblock(f: () => Unit) =
    copy(unblockHandlers = f :: unblockHandlers)

  private def addHandlers(conn: Connection) = {
    shutdownHandlers.foreach(f => conn.addShutdownListener(new ShutdownListener {
      def shutdownCompleted(cause: ShutdownSignalException) = f(cause)
    }))
    blockHandlers.foreach(f => conn.addBlockedListener(new BlockedListener {
      def handleBlocked(reason: String) = f(reason)
      def handleUnblocked { }
    }))
    unblockHandlers.foreach(f => conn.addBlockedListener(new BlockedListener {
      def handleBlocked(reason: String) { }
      def handleUnblocked = f()
    }))
    conn
  }

  def obtain: () => Connection =
    () => {
      val conn = connectionRef.get()
      if (conn == null || !conn.isOpen) {
        val newConn = factory.newConnection()
        if (connectionRef.compareAndSet(conn, newConn)) addHandlers(newConn) else {
          newConn.abort()
          obtain()
        }
      } else conn
    }

  def channel: Chan = {
    def await(attempt: Int = 0): Chan =
      try Chan(obtain().createChannel()) catch {
        case NonFatal(_) =>
          Thread.sleep(attempt * attempt * 1000)
          await(attempt + 1)
      }
    await()
  }
}
