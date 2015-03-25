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

import example.sinks.Sink
import example.sinks.kafka.KafkaSink
import example.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream

class KafkaExample(config: ApplicationConfig) extends SparkStreamingApplication with SparkStreamingKafkaSupport {

  import example.WordCount._

  override def sparkConfig: SparkConfig = config.spark

  override def sparkStreamingConfig: SparkStreamingConfig = config.sparkStreaming

  override def sparkStreamingKafkaConfig: SparkStreamingKafkaConfig = config.sparkStreamingKafka

  def start(): Unit = {
    withSparkStreamingContext {
      (sparkContext, sparkStreamingContext) =>

        val configVar = sparkContext.broadcast(config)

        val lines = createDirectStream(sparkStreamingContext, config.inputTopic)

        val words = mapLines(lines, config.stopWords)

        val results = countWords(words, Seconds(config.windowDuration), Seconds(config.slideDuration))

        storeResults(results, configVar)
    }
  }

  private def storeResults(results: DStream[WordCount], config: Broadcast[ApplicationConfig]): Unit = {
    results.foreachRDD { rdd =>
      rdd.foreachPartition { partition =>
        Sink.using(KafkaSink(config.value.sinkKafka)) { sink =>
          partition.foreach { wordCount =>
            sink.write(config.value.outputTopic, wordCount.toString)
          }
        }
      }
    }
  }
}

object KafkaExample {

  def main(args: Array[String]): Unit = {
    val config = ApplicationConfig()

    val kafkaExample = new KafkaExample(config)
    kafkaExample.start()
  }

}
