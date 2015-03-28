package example

import example.sinks.Sink
import example.sinks.kafka.KafkaSink

class JobContext(val config: JobConfig) extends Serializable {

  @transient
  lazy val sink: Sink = KafkaSink(config.sinkKafka)

  // TODO: register shutdown hook and close sink

}

object JobContext {
  def apply(config: JobConfig): JobContext = new JobContext(config)
}
