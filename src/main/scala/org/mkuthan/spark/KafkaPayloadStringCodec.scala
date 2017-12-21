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

import scala.util.Failure
import scala.util.Success

import com.twitter.bijection.Injection
import com.twitter.bijection.StringCodec
import org.apache.log4j.Logger

class KafkaPayloadStringCodec extends Serializable {

  @transient lazy private val logger = Logger.getLogger(getClass)
  @transient lazy implicit private val stringInjection = StringCodec.utf8

  def decodeValue(payload: KafkaPayload): Option[String] = {
    val decodedTry = Injection.invert[String, Array[Byte]](payload.value)
    decodedTry match {
      case Success(record) =>
        Some(record)
      case Failure(ex) =>
        logger.warn("Could not decode payload", ex)
        None
    }
  }

  def encodeValue(value: String): KafkaPayload = {
    val encoded = Injection[String, Array[Byte]](value)
    KafkaPayload(None, encoded)
  }

}

object KafkaPayloadStringCodec {
  def apply(): KafkaPayloadStringCodec = new KafkaPayloadStringCodec
}
