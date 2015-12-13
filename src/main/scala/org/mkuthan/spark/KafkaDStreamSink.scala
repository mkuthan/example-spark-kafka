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

package org.mkuthan.spark

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.TaskContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration

class KafkaDStreamSink(createProducer: () => KafkaProducer[Array[Byte], Array[Byte]])
  extends Serializable with LazyLogging {

  // scalastyle:off method.length
  def write(ssc: StreamingContext, topic: String, stream: DStream[KafkaPayload], timeout: FiniteDuration): Unit = {
    val topicVar = ssc.sparkContext.broadcast(topic)
    val createProducerVar = ssc.sparkContext.broadcast(createProducer)

    val successCounter = ssc.sparkContext.accumulator(0L, "Success counter")
    val failureCounter = ssc.sparkContext.accumulator(0L, "Failure counter")

    // cache to speed-up processing if action fails
    stream.persist(StorageLevel.MEMORY_ONLY_SER)

    stream.foreachRDD { rdd =>
      rdd.foreachPartition { records =>
        val topic = topicVar.value
        val producer = createProducerVar.value()

        // TODO: synchronize collections, callback is called in Kafka producer IO thread
        val offsets = mutable.ArrayBuffer[(TopicPartition, Long)]()
        val exceptions = mutable.ArrayBuffer[Exception]()

        def callback = new Callback {
          override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
            if (Option(metadata).isDefined) {
              val offset = (new TopicPartition(metadata.topic, metadata.partition), metadata.offset)
              offsets += offset
              successCounter += 1
            }

            if (Option(exception).isDefined) {
              exceptions += exception
              failureCounter += 1
            }
          }
        }

        records.foreach { record =>
          producer.send(new ProducerRecord(topic, record.value), callback)
        }

        producer.flush()

        if (exceptions.isEmpty) {
          // TODO: what to do with collected offsets?
        } else {
          exceptions.foreach { ex =>
            logger.debug("Could not send message for partition", ex)
          }

          // https://github.com/apache/spark/pull/5927
          val context = TaskContext.get
          throw new KafkaDStreamSinkException(
            s"Could not send partition ${context.partitionId} to topic $topic, attempt ${context.attemptNumber}"
          )
        }
      }

      logger.debug("Messages sent {}", successCounter)
      logger.debug("Messages failed {}", fallbackStringCanBuildFrom)
    }
  }
  // scalastyle:on method.length

}

object KafkaDStreamSink {

  import scala.collection.JavaConversions._

  def apply(config: Map[String, String]): KafkaDStreamSink = {

    val KEY_SERIALIZER = "org.apache.kafka.common.serialization.ByteArraySerializer"
    val VALUE_SERIALIZER = "org.apache.kafka.common.serialization.ByteArraySerializer"

    val defaultConfig = Map(
      "key.serializer" -> KEY_SERIALIZER,
      "value.serializer" -> VALUE_SERIALIZER
    )

    val f = () => {
      val producer = new KafkaProducer[Array[Byte], Array[Byte]](defaultConfig ++ config)

      sys.addShutdownHook {
        producer.close()
      }

      producer
    }

    new KafkaDStreamSink(f)
  }
}

class KafkaDStreamSinkException(message: String) extends RuntimeException(message)
