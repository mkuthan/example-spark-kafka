package example

import java.nio.file.Files

import kafka.serializer.StringDecoder
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

object KafkaExample {

  private val master = "local[4]"
  private val appName = "example-kafka"

  private val batchDuration = Seconds(10)
  private val checkpointDir = Files.createTempDirectory(appName).toString

  def main(args: Array[String]) = {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    val ssc = new StreamingContext(conf, batchDuration)
    ssc.checkpoint(checkpointDir)

    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")
    val kafkaTopics = Set("input")

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, kafkaTopics)

    messages.foreachRDD { (rdd, time) =>
      println(s"RDD time: $time")

      val offsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsets.foreach(println)

      rdd.foreach { case (key, value) =>
        println(s"$key: $value")
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
