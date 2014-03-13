package bunnyhop

import com.rabbitmq.client.{
  BlockedListener, ConnectionFactory, Connection, Channel, ShutdownListener, ShutdownSignalException
}

import scala.util.control.NonFatal
import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.atomic.AtomicReference

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
