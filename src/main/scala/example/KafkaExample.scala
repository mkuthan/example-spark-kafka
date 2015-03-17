package example

import java.nio.file.Files

import com.typesafe.config.ConfigFactory
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

import scala.collection.JavaConversions._

object KafkaExample {

  def main(args: Array[String]) = {
    val conf = ConfigFactory.load()

    val brokers = conf.getString("example.brokers")
    val inputTopic = conf.getString("example.topics.input")
    val outputTopic = conf.getString("example.topics.output")

    val batchDuration = Seconds(10)
    val checkpointDir = Files.createTempDirectory(getClass.getSimpleName).toString

    val producerConfig = Map(
      "bootstrap.servers" -> brokers,
      "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
    )

    val sparkConfig = new SparkConf()
      .setMaster("local[4]")
      .setAppName("example-kafka")

    val ssc = new StreamingContext(sparkConfig, batchDuration)
    ssc.checkpoint(checkpointDir)

    val producerConfigVar = ssc.sparkContext.broadcast(producerConfig)
    val outputTopicVar = ssc.sparkContext.broadcast(outputTopic)

    val kafkaParams = Map("metadata.broker.list" -> brokers)
    val kafkaTopics = Set(inputTopic)

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, kafkaTopics)

    messages.foreachRDD { (rdd, time) =>
      //println(s"RDD time: $time")

      val offsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      //offsets.foreach(println)

      rdd.foreachPartition { partition =>
        withKafkaProducer { kafkaProducer =>
          partition.foreach { case (key, value) =>
            kafkaProducer.send(
              new ProducerRecord(outputTopicVar.value, key, value)
            )
          }
        }
      }
    }

    ssc.start()
    ssc.awaitTermination()

    def withKafkaProducer[A](f: KafkaProducer[String, String] => A): A = {
      val producer = new KafkaProducer[String, String](producerConfigVar.value)
      try {
        f(producer)
      } finally {
        producer.close()
      }
    }
  }


}
