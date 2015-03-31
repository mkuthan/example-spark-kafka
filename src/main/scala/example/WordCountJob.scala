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

import example.sinks.DStreamSink
import example.sinks.kafka.KafkaDStreamSink
import example.sources.DStreamSource
import example.sources.kafka.KafkaDStreamSource
import example.spark._

class WordCountJob(
                    config: JobConfig,
                    source: DStreamSource,
                    sink: DStreamSink[WordCount])
  extends SparkStreamingApplication {

  import example.WordCount._

  override def sparkConfig: SparkConfig = config.spark

  override def sparkStreamingConfig: SparkStreamingConfig = config.sparkStreaming

  def start(): Unit = {
    withSparkStreamingContext {
      (sc, ssc) =>

        val lines = source.createSource(ssc, config.inputTopic)

        implicit val configVar = sc.broadcast(config)

        val words = mapLines(lines)

        val countedWords = countWords(words)

        sink.write(ssc, config.outputTopic, countedWords)
    }
  }
}

object WordCountJob {

  def main(args: Array[String]): Unit = {
    val config = JobConfig()

    val source = KafkaDStreamSource(config.sourceKafka)
    val sink = KafkaDStreamSink[WordCount](config.sinkKafka)

    val streamingJob = new WordCountJob(config, source, sink)
    streamingJob.start()
  }

}
