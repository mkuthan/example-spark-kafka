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

class ApplicationConfig(config: Config) extends Serializable {

  private val applicationConfig = config.getConfig("example")

  val inputTopic: String = applicationConfig.getString("input.topic")

  val outputTopic: String = applicationConfig.getString("output.topic")

  val stopWords: Set[String] = applicationConfig.getStringList("stopWords").toSet

  val windowDuration: Long = applicationConfig.getDuration("windowDuration", TimeUnit.SECONDS)

  val slideDuration: Long = applicationConfig.getDuration("slideDuration", TimeUnit.SECONDS)

  val spark: SparkConfig =
    SparkConfig(applicationConfig.getConfig("spark"))

  val sparkStreaming: SparkStreamingConfig =
    SparkStreamingConfig(applicationConfig.getConfig("sparkStreaming"))

  val sparkStreamingKafka: SparkStreamingKafkaConfig =
    SparkStreamingKafkaConfig(applicationConfig.getConfig("sparkStreamingKafka"))

  val sinkKafka: KafkaSinkConfig =
    KafkaSinkConfig(applicationConfig.getConfig("sinks.kafka"))

}

object ApplicationConfig {

  def apply(): ApplicationConfig = apply(ConfigFactory.load())

  def apply(config: Config): ApplicationConfig = new ApplicationConfig(config)

}
