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

import scala.concurrent.duration.FiniteDuration

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

object WordCount {

  type WordCount = (String, Int)

  def countWords(
      ssc: StreamingContext,
      lines: DStream[String],
      stopWords: Set[String],
      windowDuration: FiniteDuration,
      slideDuration: FiniteDuration
  ): DStream[WordCount] = {

    import scala.language.implicitConversions
    implicit def finiteDurationToSparkDuration(value: FiniteDuration): Duration = Duration(value.toMillis)

    val sc = ssc.sparkContext

    val stopWordsVar = sc.broadcast(stopWords)
    val windowDurationVar = sc.broadcast(windowDuration)
    val slideDurationVar = sc.broadcast(slideDuration)

    val words = lines
      .transform(splitLine)
      .transform(skipEmptyWords)
      .transform(toLowerCase)
      .transform(skipStopWords(stopWordsVar))

    val wordCounts = words
      .map(word => (word, 1))
      .reduceByKeyAndWindow(_ + _, _ - _, windowDurationVar.value, slideDurationVar.value)

    wordCounts
      .transform(skipEmptyWordCounts)
      .transform(sortWordCounts)
  }

  def toLowerCase: RDD[String] => RDD[String] =
    (words: RDD[String]) => words.map(word => word.toLowerCase)

  def splitLine: RDD[String] => RDD[String] =
    (lines: RDD[String]) => lines.flatMap(line => line.split("[^\\p{L}]"))

  def skipEmptyWords: RDD[String] => RDD[String] =
    (words: RDD[String]) => words.filter(word => !word.isEmpty)

  def skipStopWords: Broadcast[Set[String]] => RDD[String] => RDD[String] =
    (stopWords: Broadcast[Set[String]]) => (words: RDD[String]) => words.filter(word => !stopWords.value.contains(word))

  def skipEmptyWordCounts: RDD[(String, Int)] => RDD[(String, Int)] =
    (wordCounts: RDD[WordCount]) => wordCounts.filter(wordCount => wordCount._2 > 0)

  def sortWordCounts: RDD[(String, Int)] => RDD[(String, Int)] =
    (wordCounts: RDD[WordCount]) => wordCounts.sortByKey()

}
