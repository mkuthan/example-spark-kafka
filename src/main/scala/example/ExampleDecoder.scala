package example

import kafka.serializer.Decoder
import kafka.utils.VerifiableProperties

class ExampleDecoder(props: VerifiableProperties = null) extends Decoder[String] {

  override def fromBytes(bytes: Array[Byte]): String = {
    new String(bytes, "UTF8")
  }
}
