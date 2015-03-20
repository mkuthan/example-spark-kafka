// Copyright (C) 2011-2012 the original author or authors.
// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

  val BatchDuration = 10L

  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load()

    val brokers = conf.getString("example.brokers")
    val inputTopic = conf.getString("example.topics.input")
    val outputTopic = conf.getString("example.topics.output")

    val batchDuration = Seconds(BatchDuration)
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

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, kafkaTopics)

    messages.foreachRDD { (rdd, time) =>
      val offsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

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
