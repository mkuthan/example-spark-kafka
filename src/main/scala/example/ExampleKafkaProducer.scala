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

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.JavaConversions._

object ExampleKafkaProducer {

  def main(args: Array[String]): Unit = {
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
