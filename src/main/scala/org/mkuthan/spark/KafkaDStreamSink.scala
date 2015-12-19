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

import java.util.concurrent.Future

import com.typesafe.scalalogging.Logger
import org.apache.kafka.clients.producer.{Callback, ProducerRecord, RecordMetadata}
import org.apache.spark.TaskContext
import org.apache.spark.streaming.dstream.DStream
import org.slf4j.LoggerFactory

class KafkaDStreamSink(dstream: DStream[KafkaPayload]) {

  def sendToKafka(config: Map[String, String], topic: String): Unit = {
    dstream.foreachRDD { rdd =>
      rdd.foreachPartition { records =>
        val logger = Logger(LoggerFactory.getLogger(classOf[KafkaDStreamSink]))

        val producer = KafkaProducerFactory.getOrCreateProducer(config)

        // ugly hack: https://github.com/apache/spark/pull/5927
        val context = TaskContext.get

        val callback = new ExceptionHandler
        var lastMetadata: Option[Future[RecordMetadata]] = None

        logger.debug(s"Send Spark partition: ${context.partitionId} to Kafka topic: $topic")
        records.foreach { record =>
          callback.throwExceptionIfAny()

          val metadata = producer.send(new ProducerRecord(topic, record.key.orNull, record.value), callback)
          lastMetadata = Some(metadata)
        }

        // workaround for Kafka 0.8.x
        logger.debug(s"Flush Spark partition: ${context.partitionId} to Kafka topic: $topic")
        lastMetadata.foreach { metadata => metadata.get() }

        // available for Kafka 0.9.x
        // producer.flush()
      }
    }
  }
}

object KafkaDStreamSink {

  import scala.language.implicitConversions

  implicit def createKafkaDStreamSink(dstream: DStream[KafkaPayload]): KafkaDStreamSink = {
    new KafkaDStreamSink(dstream)
  }

}


class ExceptionHandler extends Callback {

  import java.util.concurrent.atomic.AtomicReference

  private val lastException = new AtomicReference[Option[Exception]](None)

  override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = lastException.set(Option(exception))

  def throwExceptionIfAny(): Unit = lastException.getAndSet(None).foreach(ex => throw ex)

}


