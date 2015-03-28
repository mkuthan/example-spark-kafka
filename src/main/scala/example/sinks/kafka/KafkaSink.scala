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

package example.sinks.kafka

import example.sinks.Sink
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.JavaConversions._

class KafkaSink(producer: KafkaProducer[String, String]) extends Sink {

  override def write(topic: String, value: String): Unit = {
    producer.send(new ProducerRecord[String, String](topic, value))
    ()
  }

  override def close(): Unit = {
    producer.close()
  }

}

object KafkaSink {

  private val KeySerializer = "org.apache.kafka.common.serialization.StringSerializer"
  private val ValueSerializer = "org.apache.kafka.common.serialization.StringSerializer"

  def apply(config: KafkaSinkConfig): KafkaSink = {
    val producer = new KafkaProducer[String, String](
      Map(
        "bootstrap.servers" -> config.bootstrapServers,
        "acks" -> config.acks,
        "key.serializer" -> KeySerializer,
        "value.serializer" -> ValueSerializer
      )
    )

    new KafkaSink(producer)
  }

}

