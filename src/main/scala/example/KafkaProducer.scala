package example

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.JavaConversions._

object KafkaProducer {

  def main(args: Array[String]) = {
    val conf = ConfigFactory.load()

    val numberOfEvents = 1000

    val brokers = conf.getString("example.brokers")
    val inputTopic = conf.getString("example.topics.input")

    val producerConfig = Map(
      "bootstrap.servers" -> brokers,
      "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
    )

    val producer = new KafkaProducer[String, String](producerConfig)

    (1 to numberOfEvents).foreach { i =>
      producer.send(
        new ProducerRecord(inputTopic, s"$i", s"${System.currentTimeMillis}")
      )
    }

  }

}
