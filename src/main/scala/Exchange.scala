package bunnyhop

import com.rabbitmq.client.AMQP
import scala.collection.JavaConverters._
import java.util.{ Map => JMap }

/** An Exchange is a description of what a publisher may publish to */
case class Exchange(name: String, chan: Chan) {
  def publish(
    payload: Array[Byte],
    routingKey: String = "",
    headers: Map[String, Any] = Map.empty[String, Any],
    properties: AMQP.BasicProperties = null) =
      chan.underlying.basicPublish(
        name, routingKey, Option(properties).orElse(
          if (headers.isEmpty) None else Some(
            new AMQP.BasicProperties().builder()
              .headers(headers.asJava.asInstanceOf[JMap[String, Object]])
              .build())
        ).orNull, payload)

  def delete(ifUnused: Boolean = false) =
    chan.underlying.exchangeDelete(name, ifUnused)
}
