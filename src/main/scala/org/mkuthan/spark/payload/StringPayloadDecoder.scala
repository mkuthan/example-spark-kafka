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

import org.apache.spark.rdd.RDD

class StringPayloadDecoder extends PayloadDecoder[String, String] {

  override def decode(payload: RDD[Payload]): RDD[(String, String)] = {
    payload.map(p => (decode(p.key), decode(p.value))
  }


  override def decodeValue(payload: RDD[Payload]): RDD[String] = {
    payload.map(p => decode(p.value))
  }

  private def decode(bytes: Array[Byte]): String = new String(bytes, "UTF8")

}

object StringPayloadDecoder {
  def apply(): StringPayloadDecoder = new StringPayloadDecoder()
}

