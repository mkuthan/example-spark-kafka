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

import example.kafka.KafkaSink
import example.spark.SparkApplication
import org.apache.spark.streaming.kafka._

object KafkaExample {

  def main(args: Array[String]): Unit = {
    val applicationConfig = ApplicationConfig()
    val sparkApplication = SparkApplication(applicationConfig.spark, applicationConfig.inputTopic)

    val applicationConfigVar = sparkApplication.broadcast(applicationConfig)

    val messages = sparkApplication.createDirectStream

    messages.foreachRDD { (rdd, time) =>
      val offsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      rdd.foreachPartition { partition =>
        withKafkaProducer { sink =>
          partition.foreach { case (key, value) =>
            sink.write(applicationConfigVar.value.outputTopic, key, value)
          }
        }
      }
    }

    sparkApplication.start()
    sparkApplication.awaitTermination()

    def withKafkaProducer[A](f: Sink => A): A = {
      val kafkaSink = KafkaSink(applicationConfigVar.value.kafkaProducer)
      try {
        f(kafkaSink)
      } finally {
        kafkaSink.close()
      }
    }
  }


}
