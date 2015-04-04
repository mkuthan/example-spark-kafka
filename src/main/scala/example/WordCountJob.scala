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

class WordCountJob(
                    config: JobConfig,
                    source: DStreamSource,
                    sink: DStreamSink,
                    decoder: StringPayloadDecoder,
                    encoder: StringPayloadEncoder
                    )
  extends SparkStreamingApplication {

  override def sparkConfig: SparkConfig = config.spark

  override def sparkStreamingConfig: SparkStreamingConfig = config.sparkStreaming

  def start(): Unit = {
    withSparkStreamingContext { (sc, ssc) =>

      val encoderVar = sc.broadcast(encoder)
      val decoderVar = sc.broadcast(decoder)

      val payload = source.createSource(ssc, config.inputTopic)
      val lines = payload.transform(p => decoderVar.value.decodeValue(p))

      val countedWords = WordCount.countWords(
        lines,
        sc.broadcast(config.stopWords),
        sc.broadcast(config.windowDuration),
        sc.broadcast(config.slideDuration)
      )

      val output = countedWords.
        map(cw => s"${cw._1}: ${cw._2}").
        transform(cws => encoderVar.value.encodeValue(cws))

      sink.write(ssc, config.outputTopic, output)
    }
  }

}

object WordCountJob {

  def main(args: Array[String]): Unit = {
    val config = JobConfig()

    val decoder = StringPayloadDecoder()
    val encoder = StringPayloadEncoder()

    val source = KafkaDStreamSource(config.sourceKafka)
    val sink = KafkaDStreamSink(config.sinkKafka)

    val streamingJob = new WordCountJob(config, source, sink, decoder, encoder)
    streamingJob.start()
  }

}
