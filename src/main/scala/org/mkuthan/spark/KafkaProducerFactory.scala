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

import scala.collection.mutable

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.log4j.Logger

object KafkaProducerFactory {

  import scala.collection.JavaConverters._

  private val Log = Logger.getLogger(getClass)

  private val Producers = mutable.Map[Map[String, Object], KafkaProducer[Array[Byte], Array[Byte]]]()

  def getOrCreateProducer(config: Map[String, Object]): KafkaProducer[Array[Byte], Array[Byte]] = {

    val defaultConfig = Map(
      "key.serializer" -> "org.apache.kafka.common.serialization.ByteArraySerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.ByteArraySerializer"
    )

    val finalConfig = defaultConfig ++ config

    Producers.getOrElseUpdate(
      finalConfig, {
        Log.info(s"Create Kafka producer , config: $finalConfig")
        val producer = new KafkaProducer[Array[Byte], Array[Byte]](finalConfig.asJava)

        sys.addShutdownHook {
          Log.info(s"Close Kafka producer, config: $finalConfig")
          producer.close()
        }

        producer
      })
  }
}

