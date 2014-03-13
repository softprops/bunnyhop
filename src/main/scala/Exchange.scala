package bunnyhop

import com.rabbitmq.client.AMQP

/** An Exchange is a description of what a publisher may publish to */
case class Exchange(name: String, chan: Chan) {
  def publish(
    payload: Array[Byte],
    routingKey: String = "",
    properties: AMQP.BasicProperties = null) =
      chan.underlying.basicPublish(
        name, routingKey, properties, payload)
}

