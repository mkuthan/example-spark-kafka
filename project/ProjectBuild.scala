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

import sbt.Keys._
import sbt._

object ProjectBuild extends Build {

  object Versions {
    val kafka = "0.8.2.1"
    val spark = "1.6.0"
  }

  val projectName = "example-spark-kafka"

  val commonSettings = Seq(
    version := "1.0",
    organization := "http://mkuthan.github.io/",
    scalaVersion := "2.11.7",
    fork := true,
    parallelExecution in Test := false,
    cancelable in Global := true
  )

  val commonScalacOptions = Seq(
    "-deprecation",
    "-encoding", "UTF-8",
    "-feature",
    "-unchecked",
    "-Xfatal-warnings",
    "-Xfuture",
    "-Xlint",
    "-Yno-adapted-args",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen",
    "-Ywarn-unused-import"
  )

  val commonLibraryDependencies = Seq(
    "org.apache.kafka" %% "kafka" % Versions.kafka,
    "org.apache.kafka" % "kafka-clients" % Versions.kafka,

    "org.apache.spark" %% "spark-core" % Versions.spark,
    "org.apache.spark" %% "spark-streaming" % Versions.spark,
    "org.apache.spark" %% "spark-streaming-kafka" % Versions.spark,

    "com.twitter" %% "bijection-avro" % "0.8.1",
    "com.twitter" %% "chill-avro" % "0.7.2",

    "com.typesafe" % "config" % "1.2.1",
    "net.ceedubs" %% "ficus" % "1.1.1",

    "ch.qos.logback" % "logback-classic" % "0.9.24",

    "org.scalatest" %% "scalatest" % "2.2.4" % "test"
  )

  val commonExcludeDependencies = Seq(
    "org.slf4j" % "slf4j-log4j12"
  )

  lazy val main = Project(projectName, base = file("."))
    .settings(commonSettings)
    .settings(scalacOptions ++= commonScalacOptions)
    .settings(libraryDependencies ++= commonLibraryDependencies)
    .settings(excludeDependencies ++= commonExcludeDependencies)
}

