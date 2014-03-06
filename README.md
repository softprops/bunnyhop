# bunnyhop

[![Build Status](https://travis-ci.org/softprops/bunnyhop.png?branch=master)](https://travis-ci.org/softprops/bunnyhop)

## rough sketches

```scala
import bunnyhop._
val channel = Connector().channel
val queue = channel.queue("example", autoDelete = true)
val exchange = queue.defaultExchange

// sub
queue.subscribe {
  case (_, _, bytes) => handle(bytes)
}

// pub
exchange.publish(
  "test".getBytes, routingKey = queue.name)
```

fanout 

```scala
import bunnyhop._

val channel = Connector().channel

// pub
val exchange = channel.fanout("logs")

// sub
channel.queue("app", autoDelete = true).bind(exchange).subscribe {
  case (_, _, bytes) => handle(bytes)
}

channel.queue("api", autoDelete = true).bind(exchange).subscribe {
  case (_, _, bytes) => handle(bytes)
}

exchange.publish("test".getBytes)
```

topic

```scala
import bunnyhop._

val channel = Connector().channel

val exchange = channel.topic("topics", autoDelete = true)

channel.queue("", exclusive = true).bind(exchange, routingKey = "puppies").subscribe {
  case (_, _, bytes) => handle(bytes)
}

channel.queue("sports").bind(exchange, routingKey = "sports.tennis.#").subscribe {
  case (_, _, bytes) => handle(bytes)
}

channel.queue("window.sports").bind(exchange, routingKey = "#.winter").subscribe {
  case (_, _, bytes) => handle(bytes)
}


exchange.publish("ski".getBytes, routingKey = "uk.winter")
  .publish("pug".getBytes, routingKey = "puppies")
  .publish("tournament is happening".getBytes, routingKey = "sports.tennis.uk")
```

Doug Tangren (softprops) 2014
