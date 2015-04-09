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

import example.WordCount.WordCount
import org.apache.spark.rdd.RDD
import org.mkuthan.spark.SparkStreamingSpec
import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}

import scala.collection.mutable
import scala.concurrent.duration._

class WordCountSpec extends FlatSpec with SparkStreamingSpec with GivenWhenThen with Matchers with Eventually {

  val DEFAULT_WINDOW_DURATION = 4.seconds
  val DEFAULT_SLIDE_DURATION = 2.seconds
  val DEFAULT_STOP_WORDS = Set[String]()

  // default timeout for eventually trait
  implicit override val patienceConfig =
    PatienceConfig(timeout = scaled(Span(1500, Millis)))

  "Sample set" should "be counted over sliding window" in {
    Given("streaming context is initialized")
    val input = mutable.Queue[RDD[String]]()

    @volatile
    var output = Array[WordCount]()

    WordCount.countWords(
      ssc.queueStream(input),
      sc.broadcast(DEFAULT_STOP_WORDS),
      sc.broadcast(DEFAULT_WINDOW_DURATION),
      sc.broadcast(DEFAULT_SLIDE_DURATION)
    ).foreachRDD(rdd => output = rdd.collect())

    ssc.start()

    When("first set of words queued")
    input += sc.makeRDD(Seq("apache"))

    Then("words counted after first slide")
    clock.advance(DEFAULT_SLIDE_DURATION)
    eventually {
      output should contain only (
        ("apache", 1))
    }

    When("second set of words queued")
    input += sc.makeRDD(Seq("apache", "spark"))

    Then("words counted after second slide")
    clock.advance(DEFAULT_SLIDE_DURATION)
    eventually {
      output should contain only(
        ("apache", 2),
        ("spark", 1))
    }

    When("nothing more queued")

    Then("word counted after third slide")
    clock.advance(DEFAULT_SLIDE_DURATION)
    eventually {
      output should contain only(
        ("apache", 1),
        ("spark", 1))
    }

    When("nothing more queued")

    Then("word counted after fourth slide")
    clock.advance(DEFAULT_SLIDE_DURATION)
    eventually {
      output shouldBe empty
    }

    //ssc.awaitTermination()
  }

  "Line" should "be split into words" in {
    val line = Seq("To be or not to be, that is the question")

    val words = WordCount.splitLine(sc.parallelize(line)).collect()

    words shouldBe Array("To", "be", "or", "not", "to", "be", "", "that", "is", "the", "question")
  }

  "Empty words" should "be skipped" in {
    val words = Seq("before", "", "after")

    val filteredWords = WordCount.skipEmptyWords(sc.parallelize(words)).collect()

    filteredWords shouldBe Array("before", "after")
  }

  "Stop words" should "be skipped" in {
    val line = Seq("The", "Beatles", "were", "an", "English", "rock", "band")
    val stopWords = Set("The", "an")

    val filteredWords = WordCount.skipStopWords(sc.broadcast(stopWords))(sc.parallelize(line)).collect()

    filteredWords shouldBe Array("Beatles", "were", "English", "rock", "band")
  }

  "Words" should "be lowercased" in {
    val words = Seq("The", "Beatles", "were", "an", "English", "rock", "band")

    val lcWords = WordCount.toLowerCase(sc.parallelize(words)).collect()

    lcWords shouldBe Array("the", "beatles", "were", "an", "english", "rock", "band")
  }

  "Empty word counts" should "be skipped" in {
    val wordCounts = Seq(("one", 1), ("zero", 0), ("two", 2))

    val filteredWordCounts = WordCount.skipEmptyWordCounts(sc.parallelize(wordCounts)).collect()

    filteredWordCounts shouldBe Array(("one", 1), ("two", 2))
  }

  "Word counts" should "be sorted" in {
    val wordCounts = Seq(("one", 1), ("zero", 0), ("two", 2))

    val sortedWordCounts = WordCount.sortWordCounts(sc.parallelize(wordCounts)).collect()

    sortedWordCounts shouldBe Array(("one", 1), ("two", 2), ("zero", 0))
  }

}
