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

package example

import org.apache.spark.rdd.RDD
import org.mkuthan.spark.SparkStreamingSpec
import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}

import scala.collection.mutable
import scala.concurrent.duration._

class WordCountSpec extends FlatSpec with SparkStreamingSpec with GivenWhenThen with Matchers with Eventually {

  val windowDuration = 4L
  val slideDuration = 2L

  // default timeout for eventually trait
  implicit override val patienceConfig =
    PatienceConfig(timeout = scaled(Span(1500, Millis)))

  "Sample set" should "be counted" in {
    Given("stop words")
    val stopWords = Set("apache")

    Given("streaming context is initialized")
    val input = mutable.Queue[RDD[String]]()

    @volatile
    var output = Array[WordCount]()

    WordCount.countWords(
      ssc.queueStream(input),
      sc.broadcast(stopWords),
      sc.broadcast(windowDuration),
      sc.broadcast(slideDuration)
    ).foreachRDD(rdd => output = rdd.collect())

    ssc.start()

    When("first set of words queued")
    input += sc.makeRDD(Seq("apache", "spark"))

    Then("words counted after first slide")
    clock.advance(slideDuration.seconds)
    eventually {
      output should contain only (
        WordCount("spark", 1))
    }

    When("second set of words queued")
    input += sc.makeRDD(Seq("apache", "spark", "streaming"))

    Then("words counted after second slide")
    clock.advance(slideDuration.seconds)
    eventually {
      output should contain only(
        WordCount("spark", 2),
        WordCount("streaming", 1))
    }

    When("nothing more queued")

    Then("word counted after third slide")
    clock.advance(slideDuration.seconds)
    eventually {
      output should contain only(
        WordCount("spark", 1),
        WordCount("streaming", 1))
    }

    When("nothing more queued")

    Then("word counted after fourth slide")
    clock.advance(slideDuration.seconds)
    eventually {
      output shouldBe empty
    }

  }

}
