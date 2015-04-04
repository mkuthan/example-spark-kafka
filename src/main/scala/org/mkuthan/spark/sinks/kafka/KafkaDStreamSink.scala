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

package org.mkuthan.spark.sinks.kafka

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.mkuthan.spark.sinks.DStreamSink

import scala.reflect.ClassTag

class KafkaDStreamSink[K: ClassTag, V: ClassTag](config: KafkaDStreamSinkConfig) extends DStreamSink[K, V] {

  private val producer = new KafkaProducerSingleton[K, V](
    Map(
      "bootstrap.servers" -> config.bootstrapServers,
      "acks" -> config.acks,
      "key.serializer" -> config.keySerializer,
      "value.serializer" -> config.valueSerializer
    )
  )

  override def write(ssc: StreamingContext, topic: String, stream: DStream[(K, V)]): Unit = {
    val topicVar = ssc.sparkContext.broadcast(topic)
    val producerVar = ssc.sparkContext.broadcast(producer)

    stream.foreachRDD { rdd =>
      rdd.foreach { record =>
        val topic = topicVar.value
        val producer = producerVar.value.producerHolder

        // TODO: callback handling
        producer.send(new ProducerRecord[K, V](topic, record._1, record._2))

        // TODO: handle record metadata
        ()
      }
    }
  }

}

object KafkaDStreamSink {
  def apply[K: ClassTag, V: ClassTag](config: KafkaDStreamSinkConfig): KafkaDStreamSink[K, V] = new KafkaDStreamSink[K, V](config)
}
