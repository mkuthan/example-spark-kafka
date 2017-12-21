# Apache Spark and Apache Kafka integration example

[![Build Status](https://travis-ci.org/mkuthan/example-spark-kafka.svg?branch=master)](https://travis-ci.org/mkuthan/example-spark-kafka) [![Coverage Status](https://img.shields.io/coveralls/mkuthan/example-spark-kafka.svg)](https://coveralls.io/r/mkuthan/example-spark-kafka?branch=master)

This example shows how to send processing results from Spark Streaming to Apache Kafka in reliable way.
The example follows Spark convention for integration with external data sinks:

    // import implicit conversions
    import org.mkuthan.spark.KafkaDStreamSink._
    
    // send dstream to Kafka
    dstream.sendToKafka(kafkaProducerConfig, topic)


## Features

* [KafkaDStreamSink](src/main/scala/org/mkuthan/spark/KafkaDStreamSink.scala) for sending streaming results to Apache Kafka in reliable way.
* Stream processing fail fast, if the results could not be sent to Apache Kafka.
* Stream processing is blocked (back pressure), if the Kafka producer is too slow.
* Stream processing results are flushed explicitly from Kafka producer internal buffer.
* Kafka producer is shared by all tasks on single JVM (see [KafkaProducerFactory](src/main/scala/org/mkuthan/spark/KafkaProducerFactory.scala)).
* Kafka producer is properly closed when Spark executor is shutdown (see [KafkaProducerFactory](src/main/scala/org/mkuthan/spark/KafkaProducerFactory.scala)).
* [Twitter Bijection](https://github.com/twitter/bijection) is used for encoding/decoding [KafkaPayload](src/main/scala/org/mkuthan/spark/KafkaPayload.scala) from/into String or Avro.

## Quickstart guide

Download latest Apache Kafka [distribution](http://kafka.apache.org/downloads.html) and un-tar it. 

Start ZooKeeper server:

    ./bin/zookeeper-server-start.sh config/zookeeper.properties

Start Kafka server:

    ./bin/kafka-server-start.sh config/server.properties

Create input topic:

    ./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic input

Create output topic:

    ./bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic output

Start Kafka producer:

    ./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic input

Start Kafka consumer:

    ./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic output

Run example application:

    sbt "runMain example.WordCountJob"

Publish a few words on input topic using Kafka console producer and check the processing result on output topic using Kafka console producer.

## References

* [Spark and Kafka integration patterns, part 1](http://mkuthan.github.io/blog/2015/08/06/spark-kafka-integration1/)
* [Spark and Kafka integration patterns, part 2](http://mkuthan.github.io/blog/2016/01/29/spark-kafka-integration2/)
* [spark-kafka-writer](https://github.com/cloudera/spark-kafka-writer)
Alternative integration library for writing processing results from Apache Spark to Apache Kafka. 
Unfortunately at the time of this writing, the library used obsolete Scala Kafka producer API and did not send processing results in reliable way.
