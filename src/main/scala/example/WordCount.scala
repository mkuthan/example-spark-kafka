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
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream

case class WordCount(word: String, count: Int) extends Serializable

object WordCount {

  def countWords(
                  lines: DStream[String],
                  stopWords: Broadcast[Set[String]],
                  windowDuration: Broadcast[Long],
                  slideDuration: Broadcast[Long]): DStream[WordCount] = {

    val reduce: (Int, Int) => Int = _ + _

    val invReduce: (Int, Int) => Int = _ - _

    val allWords = lines.flatMap { line =>
      line.split("\\s")
    }.map { word =>
      word.strip(",").strip(".").toLowerCase
    }

    val filteredWords = allWords.
      filter(word => !stopWords.value.contains(word)).
      filter(word => !word.isEmpty)

    val reducedWords = filteredWords.
      map(word => (word, 1)).
      reduceByKeyAndWindow(reduce, invReduce, Seconds(windowDuration.value), Seconds(slideDuration.value)).
      map { case (word: String, count: Int) => WordCount(word, count) }

    val relevantWords = reducedWords.filter(wordCount => wordCount.count > 0)
    val sortedWords = relevantWords.transform(rdd => rdd.sortBy(wordCount => wordCount.count, ascending = false))

    sortedWords
  }
}
