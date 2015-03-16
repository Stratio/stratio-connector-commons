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

import org.slf4j.Logger

object timer {

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
    logger.debug( s"""[millis: ${after - before}] $op""")
    t
  }

  implicit class stringChronoHelper(desc: => String) {
    def apply[T](f: => T)(implicit logger: Logger) =
      time(desc)(f)(logger)
  }

  implicit class timeHelper[T](f: => T) {
    def \\(desc: String)(implicit logger: Logger) =
      time(desc)(f)(logger)
  }

  def time[T](f: => T)(implicit logger: Logger): T =
    time(java.util.UUID.randomUUID().toString)(f)(logger)

}