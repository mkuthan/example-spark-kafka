package org.mkuthan.spark.payload

import org.apache.spark.rdd.RDD

class StringPayloadEncoder extends PayloadEncoder[String, String] {

  override def encode(value: RDD[(String, String)]): RDD[Payload] = {
    value.map(v => Payload(encode(v._1), encode(v._2)))
  }

  override def encodeValue(value: RDD[String]): RDD[Payload] = {
    value.map(v => Payload(null, encode(v)))
  }

  private def encode(s: String): Array[Byte] = s.getBytes("UTF8")

}

object StringPayloadEncoder {
  def apply(): StringPayloadEncoder = new StringPayloadEncoder()
}

