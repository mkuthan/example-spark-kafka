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

import example.WordCount.WordCount
import kafka.serializer.StringDecoder
import org.mkuthan.spark._
import org.mkuthan.spark.sinks.DStreamSink
import org.mkuthan.spark.sinks.kafka.KafkaDStreamSink
import org.mkuthan.spark.sources.DStreamSource
import org.mkuthan.spark.sources.kafka.KafkaDStreamSource

class WordCountJob(
                    config: JobConfig,
                    source: DStreamSource[String, String],
                    sink: DStreamSink[String, WordCount])
  extends SparkStreamingApplication {

  override def sparkConfig: SparkConfig = config.spark

  override def sparkStreamingConfig: SparkStreamingConfig = config.sparkStreaming

  def start(): Unit = {
    withSparkStreamingContext { (sc, ssc) =>

      val lines = source.createSource(ssc, config.inputTopic)

      val countedWords = WordCount.countWords(
        lines,
        sc.broadcast(config.stopWords),
        sc.broadcast(config.windowDuration),
        sc.broadcast(config.slideDuration)
      )

      sink.write(ssc, config.outputTopic, countedWords)
    }
  }

}

object WordCountJob {

  def main(args: Array[String]): Unit = {
    val config = JobConfig()

    val source = KafkaDStreamSource[String, String, StringDecoder, ExampleDecoder](config.sourceKafka)
    val sink = KafkaDStreamSink[String, WordCount](config.sinkKafka)

    val streamingJob = new WordCountJob(config, source, sink)
    streamingJob.start()
  }

}
