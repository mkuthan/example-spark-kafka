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

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.dstream.DStream
import org.mkuthan.spark.payload.{Payload, PayloadDecoder, PayloadEncoder}

import scala.concurrent.duration.FiniteDuration

trait WordCount {

  type WordCount = (String, Int)

  def countWords(
                  lines: DStream[String],
                  stopWords: Broadcast[Set[String]],
                  windowDuration: Broadcast[FiniteDuration],
                  slideDuration: Broadcast[FiniteDuration]): DStream[WordCount] = {

    import scala.language.implicitConversions
    implicit def finiteDurationToSparkDuration(value: FiniteDuration): Duration = new Duration(value.toMillis)

    val words = lines.
      transform(splitLine).
      transform(skipEmptyWords).
      transform(toLowerCase).
      transform(skipStopWords(stopWords))

    val wordCounts = words.
      map(word => (word, 1)).
      reduceByKeyAndWindow(_ + _, _ - _, windowDuration.value, slideDuration.value)

    wordCounts.
      transform(skipEmptyWordCounts).
      transform(sortWordCounts)
  }

  val toLowerCase = (words: RDD[String]) => words.map(word => word.toLowerCase)

  val splitLine = (lines: RDD[String]) => lines.flatMap(line => line.split("[^\\p{L}]"))

  val skipEmptyWords = (words: RDD[String]) => words.filter(word => !word.isEmpty)

  val skipStopWords = (stopWords: Broadcast[Set[String]]) => (words: RDD[String]) =>
    words.filter(word => !stopWords.value.contains(word))

  val skipEmptyWordCounts = (wordCounts: RDD[WordCount]) => wordCounts.filter(wordCount => wordCount._2 > 0)

  val sortWordCounts = (wordCounts: RDD[WordCount]) => wordCounts.sortByKey()

}

trait WordCountDecoder {
  def decodePayload(
                     payload: DStream[Payload],
                     decoder: Broadcast[PayloadDecoder[String]]):
  DStream[String] = {
    payload.transform(p => decoder.value.decode(p))
  }
}

trait WordCountEncoder {
  def encodePayload(
                     countedWords: DStream[(String, Int)],
                     encoder: Broadcast[PayloadEncoder[String]]):
  DStream[Payload] = {
    countedWords.
      map(cw => cw.toString()).
      transform(cws => encoder.value.encode(cws))
  }
}

