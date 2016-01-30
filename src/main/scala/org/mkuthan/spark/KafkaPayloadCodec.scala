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

import com.twitter.bijection.Injection
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.util.Try

trait KafkaPayloadCodec {

  def decodeValue[A](injectionBc: Broadcast[Injection[A, Array[Byte]]]): RDD[KafkaPayload] => RDD[Try[A]] = {
    (rdd: RDD[KafkaPayload]) => {
      rdd.map { payload =>
        implicit val injection = injectionBc.value
        Injection.invert[A, Array[Byte]](payload.value)
      }
    }
  }

  def encodeValue[A](injectionBc: Broadcast[Injection[A, Array[Byte]]]): RDD[A] => RDD[KafkaPayload] = {
    (rdd: RDD[A]) => {
      implicit val injection = injectionBc.value
      rdd.map { v =>
        Injection[A, Array[Byte]](v)
      }.map { v =>
        KafkaPayload(None, v)
      }
    }
  }

}
