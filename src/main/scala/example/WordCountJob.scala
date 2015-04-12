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

import org.mkuthan.spark._
import org.mkuthan.spark.payload._
import org.mkuthan.spark.sinks.DStreamSink
import org.mkuthan.spark.sinks.kafka.KafkaDStreamSink
import org.mkuthan.spark.sources.DStreamSource
import org.mkuthan.spark.sources.kafka.KafkaDStreamSource

import scala.concurrent.duration.FiniteDuration

class WordCountJob(
                    config: WordCountJobConfig,
                    source: DStreamSource,
                    sink: DStreamSink,
                    decoder: StringPayloadDecoder,
                    encoder: StringPayloadEncoder)
  extends SparkStreamingApplication
  with WordCount
  with WordCountDecoder
  with WordCountEncoder {

  override def sparkConfig: SparkApplicationConfig = config.spark

  override def sparkStreamingConfig: SparkStreamingApplicationConfig = config.sparkStreaming

  def start(): Unit = {
    withSparkStreamingContext { (sc, ssc) =>

      val lines = decodePayload(source.createSource(ssc, config.inputTopic), sc.broadcast(decoder))

      val countedWords = countWords(
        lines,
        sc.broadcast(config.stopWords),
        sc.broadcast(config.windowDuration),
        sc.broadcast(config.slideDuration)
      )

      sink.write(ssc, config.outputTopic, encodePayload(countedWords, sc.broadcast(encoder)))
    }
  }

}

object WordCountJob {

  def main(args: Array[String]): Unit = {
    val config = WordCountJobConfig()

    val decoder = StringPayloadDecoder(config.decoderString)
    val encoder = StringPayloadEncoder(config.encoderString)

    val source = KafkaDStreamSource(config.sourceKafka)
    val sink = KafkaDStreamSink(config.sinkKafka)

    val streamingJob = new WordCountJob(config, source, sink, decoder, encoder)
    streamingJob.start()
  }

}

case class WordCountJobConfig(
                               inputTopic: String,
                               outputTopic: String,
                               stopWords: Set[String],
                               windowDuration: FiniteDuration,
                               slideDuration: FiniteDuration,
                               spark: SparkApplicationConfig,
                               sparkStreaming: SparkStreamingApplicationConfig,
                               encoderString: StringPayloadEncoderConfig,
                               decoderString: StringPayloadDecoderConfig,
                               sourceKafka: Map[String, String],
                               sinkKafka: Map[String, String])
  extends Serializable

object WordCountJobConfig {

  import com.typesafe.config.{Config, ConfigFactory}
  import net.ceedubs.ficus.Ficus._
  import net.ceedubs.ficus.readers.ArbitraryTypeReader.arbitraryTypeValueReader

  def apply(): WordCountJobConfig = apply(ConfigFactory.load())

  def apply(applicationConfig: Config): WordCountJobConfig = {

    val config = applicationConfig.getConfig("wordCountJob")

    new WordCountJobConfig(
      config.as[String]("input.topic"),
      config.as[String]("output.topic"),
      config.as[Set[String]]("stopWords"),
      config.as[FiniteDuration]("windowDuration"),
      config.as[FiniteDuration]("slideDuration"),
      config.as[SparkApplicationConfig]("spark"),
      config.as[SparkStreamingApplicationConfig]("sparkStreaming"),
      config.as[StringPayloadEncoderConfig]("encoders.string"),
      config.as[StringPayloadDecoderConfig]("decoders.string"),
      config.as[Map[String, String]]("sources.kafka"),
      config.as[Map[String, String]]("sinks.kafka")
    )
  }

}

