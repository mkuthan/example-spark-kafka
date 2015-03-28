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

import example.spark._

class WordCountJob(context: JobContext) extends SparkStreamingApplication with SparkStreamingKafkaSupport {

  import example.WordCount._

  override def sparkConfig: SparkConfig = context.config.spark

  override def sparkStreamingConfig: SparkStreamingConfig = context.config.sparkStreaming

  override def sparkStreamingKafkaConfig: SparkStreamingKafkaConfig = context.config.sparkStreamingKafka

  def start(): Unit = {
    withSparkStreamingContext {
      (sparkContext, sparkStreamingContext) =>

        val lines = createDirectStream(sparkStreamingContext, context.config.inputTopic)

        implicit val contextVar = sparkContext.broadcast(context)

        val words = mapLines(lines)

        val results = countWords(words)

        storeResults(results)
    }
  }
}

object WordCountJob {

  def main(args: Array[String]): Unit = {
    val config = JobConfig()
    val context = JobContext(config)

    val streamingJob = new WordCountJob(context)
    streamingJob.start()
  }

}
