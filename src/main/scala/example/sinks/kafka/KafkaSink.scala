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

class KafkaSink(producer: KafkaProducer[Array[Byte], Array[Byte]]) extends Sink {

  override def write(topic: String, key: Array[Byte], value: Array[Byte]): Unit = {
    producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, key, value))
    ()
  }

  override def close(): Unit = {
    producer.close()
  }

}

object KafkaSink {

  private val keySerializer = "org.apache.kafka.common.serialization.ByteArraySerializer"
  private val valueSerializer = "org.apache.kafka.common.serialization.ByteArraySerializer"

  def apply(config: KafkaSinkConfig): KafkaSink = {
    val producer = new KafkaProducer[Array[Byte], Array[Byte]](
      Map(
        "bootstrap.servers" -> config.bootstrapServers,
        "acks" -> config.acks,
        "key.serializer" -> keySerializer,
        "value.serializer" -> valueSerializer
      )
    )
    new KafkaSink(producer)
  }

}

