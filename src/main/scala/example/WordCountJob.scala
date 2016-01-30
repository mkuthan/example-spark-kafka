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

import com.twitter.bijection.StringCodec
import org.apache.spark.storage.StorageLevel
import org.mkuthan.spark._

import scala.concurrent.duration.FiniteDuration

class WordCountJob(config: WordCountJobConfig,
                   source: KafkaDStreamSource)
  extends SparkStreamingApplication with WordCount with KafkaPayloadCodec {

  override def sparkConfig: Map[String, String] = config.spark

  override def sparkStreamingConfig: SparkStreamingApplicationConfig = config.sparkStreaming

  def start(): Unit = {
    withSparkStreamingContext { (sc, ssc) =>
      val input = source.createSource(ssc, config.inputTopic)

      // Option 1: Array[Byte] -> String
      val valueInjection = sc.broadcast(StringCodec.utf8)

      // Option 2: Array[Byte] -> Specific Avro
      // val valueInjection = sc.broadcast(SpecificAvroCodecs[SomeAvroType])

      // Option 3: Array[Byte] -> Generic Avro
      // val valueInjection = sc.broadcast(GenericAvroCodecs[GenericRecord](someAvroSchema))

      // decode Kafka Payload (e.g: from String or Avro)
      val lines = input
        .transform(decodeValue(valueInjection))
        .flatMap(_.toOption)

      val countedWords = countWords(
        ssc,
        lines,
        config.stopWords,
        config.windowDuration,
        config.slideDuration
      )

      // encode Kafka payload (e.g: to String or Avro)
      val output = countedWords
        .map(_.toString())
        .transform(encodeValue(valueInjection))

      // cache to speed-up processing if action fails
      output.persist(StorageLevel.MEMORY_ONLY_SER)

      import org.mkuthan.spark.KafkaDStreamSink._
      output.sendToKafka(config.sinkKafka, config.outputTopic)
    }
  }

}

object WordCountJob {

  def main(args: Array[String]): Unit = {
    val config = WordCountJobConfig()

    val source = KafkaDStreamSource(config.sourceKafka)

    val streamingJob = new WordCountJob(config, source)
    streamingJob.start()
  }

}

case class WordCountJobConfig(
                               inputTopic: String,
                               outputTopic: String,
                               stopWords: Set[String],
                               windowDuration: FiniteDuration,
                               slideDuration: FiniteDuration,
                               spark: Map[String, String],
                               sparkStreaming: SparkStreamingApplicationConfig,
                               sourceKafka: Map[String, String],
                               sinkKafka: Map[String, String])
  extends Serializable

object WordCountJobConfig {

  import com.typesafe.config.{Config, ConfigFactory}
  import net.ceedubs.ficus.Ficus._
  import net.ceedubs.ficus.readers.ArbitraryTypeReader.arbitraryTypeValueReader

  def apply(): WordCountJobConfig = apply(ConfigFactory.load)

  def apply(applicationConfig: Config): WordCountJobConfig = {

    val config = applicationConfig.getConfig("wordCountJob")

    new WordCountJobConfig(
      config.as[String]("input.topic"),
      config.as[String]("output.topic"),
      config.as[Set[String]]("stopWords"),
      config.as[FiniteDuration]("windowDuration"),
      config.as[FiniteDuration]("slideDuration"),
      config.as[Map[String, String]]("spark"),
      config.as[SparkStreamingApplicationConfig]("sparkStreaming"),
      config.as[Map[String, String]]("kafkaSource"),
      config.as[Map[String, String]]("kafkaSink")
    )
  }

}
