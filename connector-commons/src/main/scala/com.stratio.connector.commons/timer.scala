/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


package com.stratio.connector.commons

import com.codahale.metrics.{MetricRegistry, Timer}
import org.slf4j.Logger

trait ABSTimer {

  def time[T](f: => T)(implicit logger: Logger): T =
    time(getUID)(f)(logger)

  def getUID[T]: String = {
    java.util.UUID.randomUUID().toString
  }

  /**
   * Evals some {{{T}}} by-name expression and get times spent on it.
   *
   * @param op Description of what {{{f}}} does
   * @param f By-name {{{T}}} expression
   * @param logger Implicit logger used to print out elapsed time
   * @tparam T Expression type
   * @return Expression value
   */
  def time[T](op: String)(f: => T)(implicit logger: Logger): T = {
    val before = System.currentTimeMillis()
    val t = f
    val after = System.currentTimeMillis()
    logger.debug( s"""millis: [${after - before}] $op""")
    t
  }

  def timeFor[T, U](timerName: String)(f: => T)(
    implicit metricRegistry: MetricRegistry, logger: Logger): T = {
    import scala.collection.JavaConversions._


    val timer = this.synchronized {
      metricRegistry.getTimers.toMap.getOrElse(timerName, {
        metricRegistry.register(timerName, new Timer())

      })
    }
    if (logger.isDebugEnabled()) {
      logger.debug(s"The process [$timerName] is starting")
    }
    val before = timer.time()
    val timeBefore = before.stop
    val t = f
    val after = before.stop()
    if (logger.isDebugEnabled()) {
      logger.debug( s"""[millis: ${after - timeBefore}] $timerName""")
    }
    t
  }

}

object timer extends ABSTimer
