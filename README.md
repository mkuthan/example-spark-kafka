# Apache Kafka and Apache Spark example

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

