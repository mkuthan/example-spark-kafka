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

import com.typesafe.config._
import org.mkuthan.spark.sinks.kafka.KafkaDStreamSinkConfig
import org.mkuthan.spark.sources.kafka.KafkaDStreamSourceConfig
import org.mkuthan.spark.{SparkConfig, SparkStreamingConfig}

import scala.collection.JavaConversions._

case class JobConfig(
                      inputTopic: String,
                      outputTopic: String,
                      stopWords: Set[String],
                      windowDuration: Long,
                      slideDuration: Long,
                      spark: SparkConfig,
                      sparkStreaming: SparkStreamingConfig,
                      sourceKafka: KafkaDStreamSourceConfig,
                      sinkKafka: KafkaDStreamSinkConfig)
  extends Serializable {
}

object JobConfig {

  def apply(): JobConfig = apply(ConfigFactory.load())

  def apply(applicationConfig: Config): JobConfig = {

    val config = applicationConfig.getConfig("example")

    new JobConfig(
      config.getString("input.topic"),
      config.getString("output.topic"),
      config.getStringList("stopWords").toSet,
      config.getDuration("windowDuration", TimeUnit.SECONDS),
      config.getDuration("slideDuration", TimeUnit.SECONDS),
      SparkConfig(config.getConfig("spark")),
      SparkStreamingConfig(config.getConfig("sparkStreaming")),
      KafkaDStreamSourceConfig(config.getConfig("sources.kafka")),
      KafkaDStreamSinkConfig(config.getConfig("sinks.kafka"))
    )
  }

}
