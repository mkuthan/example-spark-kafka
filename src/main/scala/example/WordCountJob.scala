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

import java.util.concurrent.TimeUnit

import org.mkuthan.spark._
import org.mkuthan.spark.payload._
import org.mkuthan.spark.sinks.DStreamSink
import org.mkuthan.spark.sinks.kafka.{KafkaDStreamSinkConfig, KafkaDStreamSink}
import org.mkuthan.spark.sources.DStreamSource
import org.mkuthan.spark.sources.kafka.{KafkaDStreamSourceConfig, KafkaDStreamSource}

class WordCountJob(
                    config: JobConfig,
                    source: DStreamSource,
                    sink: DStreamSink,
                    decoder: StringPayloadDecoder,
                    encoder: StringPayloadEncoder)
  extends SparkStreamingApplication {

  override def sparkConfig: SparkApplicationConfig = config.spark

  override def sparkStreamingConfig: SparkStreamingApplicationConfig = config.sparkStreaming

  def start(): Unit = {
    withSparkStreamingContext { (sc, ssc) =>

      import WordCount._

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
    val config = JobConfig()

    val decoder = StringPayloadDecoder(config.decoderString)
    val encoder = StringPayloadEncoder(config.encoderString)

    val source = KafkaDStreamSource(config.sourceKafka)
    val sink = KafkaDStreamSink(config.sinkKafka)

    val streamingJob = new WordCountJob(config, source, sink, decoder, encoder)
    streamingJob.start()
  }

}

case class JobConfig(
                      inputTopic: String,
                      outputTopic: String,
                      stopWords: Set[String],
                      windowDuration: Long,
                      slideDuration: Long,
                      spark: SparkApplicationConfig,
                      sparkStreaming: SparkStreamingApplicationConfig,
                      encoderString: StringPayloadEncoderConfig,
                      decoderString: StringPayloadDecoderConfig,
                      sourceKafka: KafkaDStreamSourceConfig,
                      sinkKafka: KafkaDStreamSinkConfig)
  extends Serializable {
}

object JobConfig {

  import com.typesafe.config.{Config, ConfigFactory}
  import scala.collection.JavaConversions._

  def apply(): JobConfig = apply(ConfigFactory.load())

  def apply(applicationConfig: Config): JobConfig = {

    val config = applicationConfig.getConfig("example")

    new JobConfig(
      config.getString("input.topic"),
      config.getString("output.topic"),
      config.getStringList("stopWords").toSet,
      config.getDuration("windowDuration", TimeUnit.SECONDS),
      config.getDuration("slideDuration", TimeUnit.SECONDS),
      SparkApplicationConfig(config.getConfig("spark")),
      SparkStreamingApplicationConfig(config.getConfig("sparkStreaming")),
      StringPayloadEncoderConfig(config.getConfig("encoders.string")),
      StringPayloadDecoderConfig(config.getConfig("decoders.string")),
      KafkaDStreamSourceConfig(config.getConfig("sources.kafka")),
      KafkaDStreamSinkConfig(config.getConfig("sinks.kafka"))
    )
  }

}

