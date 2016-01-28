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

import org.apache.spark.streaming._
import org.scalatest._

import scala.concurrent.duration.FiniteDuration

trait SparkStreamingSpec extends SparkSpec {
  this: Suite =>

  import java.nio.file.Files
  import java.util.concurrent.TimeUnit

  import ClockWrapper._

  private var _ssc: StreamingContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    _ssc = new StreamingContext(sc, batchDuration)
    _ssc.checkpoint(checkpointDir)
  }

  def batchDuration: Duration = Seconds(1)

  def checkpointDir: String = Files.createTempDirectory(this.getClass.getSimpleName).toUri.toString

  override def afterAll(): Unit = {
    if (_ssc != null) {
      _ssc.stop(stopSparkContext = false, stopGracefully = false)
      _ssc = null
    }

    super.afterAll()
  }

  override def sparkConfig: Map[String, String] = {
    super.sparkConfig + ("spark.streaming.clock" -> "org.apache.spark.streaming.util.ManualClock")
  }

  def ssc: StreamingContext = _ssc

  def advanceClock(timeToAdd: FiniteDuration): Unit = {
    advance(_ssc, timeToAdd)
  }

  def advanceClockOneBatch(): Unit = {
    advance(_ssc, FiniteDuration(batchDuration.milliseconds, TimeUnit.MILLISECONDS))
  }

}