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

import org.apache.spark.{SparkConf, SparkContext}

trait SparkApplication {

  private val SERIALIZER = "org.apache.spark.serializer.KryoSerializer"

  def sparkConfig: SparkApplicationConfig

  def withSparkContext(f: SparkContext => Unit): Unit = {
    val sparkConf = new SparkConf()
      .setMaster(sparkConfig.master)
      .setAppName(sparkConfig.appName)
      .set("spark.serializer", SERIALIZER)
    //.registerKryoClasses(sparkConfig.kryoClasses.map(Class.forName(_)))

    val sc = new SparkContext(sparkConf)

    f(sc)
  }

}

case class SparkApplicationConfig(master: String,
                                  appName: String,
                                  kryoClasses: Array[String])
  extends Serializable

