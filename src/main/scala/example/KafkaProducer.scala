package example

import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer}

import scala.collection.JavaConversions._

object KafkaProducer {

  def main(args: Array[String]) = {
    val numberOfEvents = 1000

    val config = Map(
      "bootstrap.servers" -> "localhost:9092",
      "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
    )

    val producer = new KafkaProducer[String, String](config)

    (1 to numberOfEvents).foreach { i =>
      producer.send(
        new ProducerRecord("input", s"$i", s"${System.currentTimeMillis}")
      )
    }

  }

}
