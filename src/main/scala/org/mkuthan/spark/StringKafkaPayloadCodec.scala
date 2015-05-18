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

package org.mkuthan.spark

import org.apache.spark.streaming.StreamingContext

import scala.util.Try

class StringKafkaPayloadCodec(config: StringKafkaPayloadCodecConfig) extends KafkaPayloadCodec[String] {

  override def decoder(ssc: StreamingContext): KafkaPayload => Try[String] = {
    payload => decode(payload.value)
  }

  override def encoder(ssc: StreamingContext): String => Try[KafkaPayload] = {
    value => Try {
      KafkaPayload(encode(value))
    }
  }

  private def decode(bytes: Array[Byte]): Try[String] = Try {
    new String(bytes, config.encoding)
  }

  private def encode(s: String): Array[Byte] = s.getBytes(config.encoding)

}

object StringKafkaPayloadCodec {
  def apply(config: StringKafkaPayloadCodecConfig): StringKafkaPayloadCodec = new StringKafkaPayloadCodec(config)
}

case class StringKafkaPayloadCodecConfig(encoding: String) extends Serializable

