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

import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success, Try}

class KafkaDStreamSink(createProducer: () => KafkaProducer[Array[Byte], Array[Byte]]) extends Serializable {

  def write(ssc: StreamingContext, topic: String, stream: DStream[KafkaPayload], timeout: FiniteDuration): Unit = {
    val topicVar = ssc.sparkContext.broadcast(topic)
    val createProducerVar = ssc.sparkContext.broadcast(createProducer)

    val successCounter = ssc.sparkContext.accumulator(0L, "Success counter")
    val failureCounter = ssc.sparkContext.accumulator(0L, "Failure counter")

    // cache to speed-up processing if action fails
    stream.persist(StorageLevel.MEMORY_ONLY_SER)

    stream.foreachRDD { rdd =>
      // TODO: report counters somewhere

      rdd.foreachPartition { records =>
        val topic = topicVar.value
        val producer = createProducerVar.value()

        val futures = records.map { record =>
          producer.send(new ProducerRecord(topic, record.value)
        }

        val results = futures.map { future =>
          Try {
            future.get(timeout.toMillis, TimeUnit.MILLISECONDS)
          } match {
            case f@Failure(ex) =>
              failureCounter += 1
              f
            case s@Success(_) =>
              successCounter += 1
              s
          }
        }

        results.collect {
          case Failure(_) =>
            failureCounter += 1
          case Success(_) =>
            successCounter += 1
        }

        // to throw or not to throw exception?
        results.foreach { case Failure(ex) => throw ex }
      }
    }
  }
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
