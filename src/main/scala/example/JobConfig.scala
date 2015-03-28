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
import example.sinks.kafka.KafkaSinkConfig
import example.spark.{SparkConfig, SparkStreamingConfig, SparkStreamingKafkaConfig}

import scala.collection.JavaConversions._

class JobConfig(applicationConfig: Config) extends Serializable {

  private val config = applicationConfig.getConfig("example")

  val inputTopic: String = config.getString("input.topic")

  val outputTopic: String = config.getString("output.topic")

  val stopWords: Set[String] = config.getStringList("stopWords").toSet

  val windowDuration: Long = config.getDuration("windowDuration", TimeUnit.SECONDS)

  val slideDuration: Long = config.getDuration("slideDuration", TimeUnit.SECONDS)

  val spark: SparkConfig =
    SparkConfig(config.getConfig("spark"))

  val sparkStreaming: SparkStreamingConfig =
    SparkStreamingConfig(config.getConfig("sparkStreaming"))

  val sparkStreamingKafka: SparkStreamingKafkaConfig =
    SparkStreamingKafkaConfig(config.getConfig("sparkStreamingKafka"))

  val sinkKafka: KafkaSinkConfig =
    KafkaSinkConfig(config.getConfig("sinks.kafka"))

}

object JobConfig {

  def apply(): JobConfig = apply(ConfigFactory.load())

  def apply(applicationConfig: Config): JobConfig = new JobConfig(applicationConfig)

}
