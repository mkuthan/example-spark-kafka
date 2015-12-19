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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{BinaryDecoder, BinaryEncoder, DecoderFactory, EncoderFactory}

import scala.util.Try

class AvroKafkaPayloadCodec(schemaRepositoryClient: SchemaRepositoryClient, subject: String)
  extends KafkaPayloadCodec[String, GenericRecord] {

  def decode(payload: KafkaPayload): Try[(Option[String], GenericRecord)] = {
    for {
      value <- createGenericRecord(payload.value, subject)
    } yield (None, value)
  }

  def encode(kv: (Option[String], GenericRecord)): Try[KafkaPayload] = {
    Try {
      KafkaPayload(None, serializeGenericRecord(kv._2))
    }
  }

  private def serializeGenericRecord(genericRecord: GenericRecord) = {
    val schema = genericRecord.getSchema
    val writer = new GenericDatumWriter[Object](schema)
    val outputStream = new ByteArrayOutputStream()
    val reuse: Option[BinaryEncoder] = None
    val encoder = EncoderFactory.get().binaryEncoder(outputStream, reuse.orNull)
    writer.write(genericRecord, encoder)
    encoder.flush()

    outputStream.toByteArray
  }

  private def createGenericRecord(data: Array[Byte], schemaName: String): Try[GenericRecord] = {
    for {
      schema <- schemaRepositoryClient.findSchema(schemaName)
      decoder <- Try {
        val reuse: Option[BinaryDecoder] = None
        DecoderFactory.get().directBinaryDecoder(new ByteArrayInputStream(data), reuse.orNull)
      }
      reader <- Try {
        new GenericDatumReader[GenericRecord](schema)
      }
      genericRecord <- Try {
        val reuse: Option[GenericRecord] = None
        reader.read(reuse.orNull, decoder)
      }
    } yield genericRecord
  }
}

object AvroKafkaPayloadCodec {
  def apply(schemaRepositoryClient: SchemaRepositoryClient, subject: String): AvroKafkaPayloadCodec = {
    new AvroKafkaPayloadCodec(schemaRepositoryClient, subject)
  }
}
