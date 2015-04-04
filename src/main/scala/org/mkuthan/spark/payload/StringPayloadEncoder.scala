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

package org.mkuthan.spark.payload

import com.typesafe.config.Config
import org.apache.spark.rdd.RDD

class StringPayloadEncoder(config: StringPayloadEncoderConfig)
  extends PayloadEncoder[String, String] {

  override def encode(value: RDD[(String, String)]): RDD[Payload] = {
    value.map(v => Payload(encode(v._1), encode(v._2)))
  }

  override def encodeValue(value: RDD[String]): RDD[Payload] = {
    // TODO: get rid of null somehow
    value.map(v => Payload(null, encode(v)))
  }

  private def encode(s: String): Array[Byte] = s.getBytes(config.encoding)

}

object StringPayloadEncoder {
  def apply(config: StringPayloadEncoderConfig): StringPayloadEncoder = new StringPayloadEncoder(config)
}

case class StringPayloadEncoderConfig(encoding: String) extends Serializable

object StringPayloadEncoderConfig {
  def apply(config: Config): StringPayloadEncoderConfig = {
    new StringPayloadEncoderConfig(
      config.getString("encoding")
    )
  }
}

