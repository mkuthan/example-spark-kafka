# Apache Kafka and Apache Spark example

[![Build Status](https://travis-ci.org/mkuthan/example-spark-kafka.svg?branch=master)](https://travis-ci.org/mkuthan/example-spark-kafka) [![Coverage Status](https://img.shields.io/coveralls/mkuthan/example-spark-kafka.svg)](https://coveralls.io/r/mkuthan/example-spark-kafka?branch=master)


Start ZooKeeper server:

```
bin/zookeeper-server-start.sh config/zookeeper.properties
```

Start Kafka server:

```
bin/kafka-server-start.sh config/server.properties
```

Create input topic:

```
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 10 --topic input
```

Create output topic:

```
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 10 --topic output
```

Start kafka producer:

```
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic input
```

Start kafka consumer:

```
bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic output
```

Spark UI

[http://localhost:4040/](http://localhost:4040/)