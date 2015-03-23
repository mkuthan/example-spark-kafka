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

package example.spark

import java.nio.file.Files

import kafka.serializer.DefaultDecoder
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka._

import scala.reflect.ClassTag


class SparkApplication(
                        streamingContext: StreamingContext,
                        kafkaParams: Map[String, String],
                        kafkaTopics: Set[String]) {

  def createDirectStream: DStream[(Array[Byte], Array[Byte])] = KafkaUtils
    .createDirectStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](
      streamingContext,
      kafkaParams,
      kafkaTopics)

  def broadcast[T: ClassTag](value: T): Broadcast[T] = streamingContext.sparkContext.broadcast(value)

  def start(): Unit = streamingContext.start()

  def awaitTermination(): Unit = streamingContext.awaitTermination()

}

object SparkApplication {

  def apply(config: SparkConfig, inputTopic: String): SparkApplication = {
    val batchDuration = Seconds(config.batchDuration)
    val checkpointDir = Files.createTempDirectory(config.appName).toString

    val sparkConfig = new SparkConf()
      .setMaster(config.master)
      .setAppName(config.appName)

    val streamingContext = new StreamingContext(sparkConfig, batchDuration)
    streamingContext.checkpoint(checkpointDir)

    val kafkaParams = Map(
      "metadata.broker.list" -> config.kafkaMetadataBrokerList,
      "auto.offset.reset" -> config.kafkaAutoOffsetReset
    )
    val kafkaTopics = Set(inputTopic)

    new SparkApplication(streamingContext, kafkaParams, kafkaTopics)
  }

}
