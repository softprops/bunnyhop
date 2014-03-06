package bunnyhop

import com.rabbitmq.client.{
  AMQP, ConnectionFactory, Connection,
  Consumer, Channel, DefaultConsumer, Envelope
}

case class Exchange(name: String, chan: Chan) {
  def publish(
    payload: Array[Byte],
    routingKey: String = "",
    properties: AMQP.BasicProperties = null) =
      chan.underlying.basicPublish(
        name, routingKey, properties, payload)
}

case class Queue(
  name: String,
  chan: Chan,
  durable: Boolean = false,
  exclusive: Boolean = false,
  autoDelete: Boolean = false) {
  chan.underlying.queueDeclare(
    name, durable, exclusive, autoDelete, null)

  def bind(ex: Exchange, routingKey: String = "") = {
    chan.underlying.queueBind(
      name,//chan.underlying.queueDeclare().getQueue(),
      ex.name, routingKey)
    this
  }

  def subscribe(
    f: (Envelope, AMQP.BasicProperties, Array[Byte]) => Unit) = {
    chan.underlying.basicConsume(
      name, false/*auto ack*/,
      new DefaultConsumer(chan.underlying) {
        override def handleDelivery(
          consumerTag: String, envelope: Envelope,
          props: AMQP.BasicProperties, body: Array[Byte]) {
            f(envelope, props, body)
        }
      }) 
  }
}

case class Chan(underlying: Channel) {
  def queue(
    name: String,
    autoDelete: Boolean = true,
    exclusive: Boolean = false) =
    Queue(name, this,
          autoDelete = autoDelete,
          exclusive = exclusive)

  def defaultExchange = Exchange("", this)

  def fanout(exchange: String) = {
    underlying.exchangeDeclare(exchange, "fanout")
    Exchange(exchange, this)
  }

  def topic(exchange: String) = {
    underlying.exchangeDeclare(exchange, "topic")
    Exchange(exchange, this)
  }

  def close() {
    underlying.close()
    underlying.getConnection.close()
  }
}

case class Connector(host: String = "localhost") {
  def obtain: () => Connection =
    () => new ConnectionFactory() {
      setHost(host)
    }.newConnection()
  def channel = Chan(obtain().createChannel())
}
