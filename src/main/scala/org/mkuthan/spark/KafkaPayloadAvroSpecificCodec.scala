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

import scala.reflect.ClassTag
import scala.util.Failure
import scala.util.Success

import com.twitter.bijection.Injection
import com.twitter.bijection.avro.SpecificAvroCodecs
import org.apache.avro.specific.SpecificRecordBase
import org.apache.log4j.Logger

class KafkaPayloadAvroSpecificCodec[A <: SpecificRecordBase : ClassTag] extends Serializable {

  @transient lazy private val logger = Logger.getLogger(getClass)
  @transient lazy implicit private val avroSpecificInjection = SpecificAvroCodecs.toBinary[A]

  def decodeValue(payload: KafkaPayload): Option[A] = {
    val decodedTry = Injection.invert[A, Array[Byte]](payload.value)
    decodedTry match {
      case Success(record) =>
        Some(record)
      case Failure(ex) =>
        logger.warn("Could not decode payload", ex)
        None
    }
  }

  def encodeValue(value: A): KafkaPayload = {
    val encoded = Injection[A, Array[Byte]](value)
    KafkaPayload(None, encoded)
  }

}

object KafkaPayloadAvroSpecificCodec {
  def apply[A <: SpecificRecordBase : ClassTag](): KafkaPayloadAvroSpecificCodec[A] =
    new KafkaPayloadAvroSpecificCodec[A]
}
