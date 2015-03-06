package com.stratio.connector.commons

import org.slf4j.Logger

object timer {

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