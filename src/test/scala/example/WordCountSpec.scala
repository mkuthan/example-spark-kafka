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

import example.WordCount._
import org.apache.spark.rdd.RDD
import org.mkuthan.spark.SparkStreamingSpec
import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}

import scala.collection.mutable
import scala.concurrent.duration._

object WordCountSpec {
  val windowDuration = 4.seconds
  val slideDuration = 2.seconds
  val stopWords = Set("the", "an", "a")

  val input = mutable.Queue[RDD[String]]()
  val output = mutable.ArrayBuffer.empty[Array[WordCount]]
}

class WordCountSpec extends FlatSpec with GivenWhenThen with Matchers with Eventually with SparkStreamingSpec {

  import WordCountSpec._

  // default timeout for eventually trait
  implicit override val patienceConfig =
    PatienceConfig(timeout = scaled(Span(5000, Millis)))

  override def beforeAll(): Unit = {
    super.beforeAll()

    WordCount.countWords(
      ssc,
      ssc.queueStream(input),
      stopWords,
      windowDuration,
      slideDuration
    ).foreachRDD { rdd =>
      output += rdd.collect()
    }

    ssc.start()
  }

  override def sparkConfig: Map[String, String] = {
    super.sparkConfig +
      ("spark.serializer" -> "org.apache.spark.serializer.KryoSerializer") +
      ("spark.kryo.registrator" -> "example.WordCountKryoRegistration")
  }

  "Apache Spark set" should "be counted over sliding window" in {
    When("first set of words queued")
    input += sc.makeRDD(Seq("apache"))

    Then("words counted after first slide")
    advanceClock(slideDuration)
    eventually {
      output.last should contain only (
        ("apache", 1))
    }

    When("second set of words queued")
    input += sc.makeRDD(Seq("apache", "spark"))

    Then("words counted after second slide")
    advanceClock(slideDuration)
    eventually {
      output.last should contain only(
        ("apache", 2),
        ("spark", 1))
    }

    When("nothing more queued")

    Then("word counted after third slide")
    advanceClock(slideDuration)
    eventually {
      output.last should contain only(
        ("apache", 1),
        ("spark", 1))
    }

    When("nothing more queued")

    Then("word counted after fourth slide")
    advanceClock(slideDuration)
    eventually {
      output.last shouldBe empty
    }
  }

  "The most famous Hamlet quote" should "be counted over sliding window" in {
    input += sc.makeRDD(Seq("To be"))
    advanceClock(slideDuration)
    eventually {
      output.last should contain only(
        ("be", 1),
        ("to", 1))
    }

    input += sc.makeRDD(Seq("or not to be,"))
    advanceClock(slideDuration)
    eventually {
      output.last should contain only(
        ("be", 2),
        ("not", 1),
        ("or", 1),
        ("to", 2))
    }

    input += sc.makeRDD(Seq(" that is the question"))
    advanceClock(slideDuration)
    eventually {
      output.last should contain only(
        ("be", 1),
        ("is", 1),
        ("not", 1),
        ("or", 1),
        ("question", 1),
        ("that", 1),
        ("to", 1))
    }

    advanceClock(slideDuration)
    eventually {
      output.last should contain only(
        ("is", 1),
        ("question", 1),
        ("that", 1))
    }

    advanceClock(slideDuration)
    eventually {
      output.last shouldBe empty
    }
  }

  "Line" should "be split into words" in {
    val line = Seq("To be or not to be, that is the question")

    val words = splitLine(sc.parallelize(line)).collect()

    words shouldBe Array("To", "be", "or", "not", "to", "be", "", "that", "is", "the", "question")
  }

  "Empty words" should "be skipped" in {
    val words = Seq("before", "", "after")

    val filteredWords = skipEmptyWords(sc.parallelize(words)).collect()

    filteredWords shouldBe Array("before", "after")
  }

  "Stop words" should "be skipped" in {
    val line = Seq("The", "Beatles", "were", "an", "English", "rock", "band")
    val stopWords = Set("The", "an")

    val filteredWords = skipStopWords(sc.broadcast(stopWords))(sc.parallelize(line)).collect()

    filteredWords shouldBe Array("Beatles", "were", "English", "rock", "band")
  }

  "Words" should "be lowercased" in {
    val words = Seq("The", "Beatles", "were", "an", "English", "rock", "band")

    val lcWords = toLowerCase(sc.parallelize(words)).collect()

    lcWords shouldBe Array("the", "beatles", "were", "an", "english", "rock", "band")
  }

  "Empty word counts" should "be skipped" in {
    val wordCounts = Seq(("one", 1), ("zero", 0), ("two", 2))

    val filteredWordCounts = skipEmptyWordCounts(sc.parallelize(wordCounts)).collect()

    filteredWordCounts shouldBe Array(("one", 1), ("two", 2))
  }

  "Word counts" should "be sorted" in {
    val wordCounts = Seq(("one", 1), ("zero", 0), ("two", 2))

    val sortedWordCounts = sortWordCounts(sc.parallelize(wordCounts)).collect()

    sortedWordCounts shouldBe Array(("one", 1), ("two", 2), ("zero", 0))
  }

}
