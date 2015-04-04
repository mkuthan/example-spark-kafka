package example

import java.util

import org.apache.kafka.common.serialization.Serializer

class ExampleSerializer extends Serializer[(String, Int)] {

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
  }

  override def serialize(topic: String, data: (String, Int)): Array[Byte] = {
    s"${data._1}: ${data._2}".getBytes
  }

  override def close(): Unit = {
  }

}
