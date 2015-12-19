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

import scala.util.Try

class StringKafkaPayloadCodec(config: StringKafkaPayloadCodecConfig) extends KafkaPayloadCodec[String, String] {

  override def decode(payload: KafkaPayload): Try[(Option[String], String)] = {
    def decode(b: Option[Array[Byte]]): Try[Option[String]] = Try {
      b.map {
        new String(_, config.encoding)
      }
    }

    for {
      key <- decode(payload.key)
      value <- decode(Some(payload.value))
    } yield (key, value.get)
  }

  override def encode(kv: (Option[String], String)): Try[KafkaPayload] = {
    def encode(s: Option[String]): Try[Option[Array[Byte]]] = Try {
      s.map {
        _.getBytes(config.encoding)
      }
    }

    for {
      key <- encode(kv._1)
      value <- encode(Some(kv._2))
    } yield KafkaPayload(key, value.get)
  }
}

object StringKafkaPayloadCodec {
  def apply(config: StringKafkaPayloadCodecConfig): StringKafkaPayloadCodec = new StringKafkaPayloadCodec(config)
}

case class StringKafkaPayloadCodecConfig(encoding: String) extends Serializable

