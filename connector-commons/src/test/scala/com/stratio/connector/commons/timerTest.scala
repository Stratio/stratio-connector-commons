package com.stratio.connector.commons

import java.util.UUID

import junit.framework.Assert
import org.junit.runner.RunWith
import org.mockito.ArgumentCaptor
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.junit.JUnitRunner
import org.slf4j.Logger


import org.mockito.Mockito._

/**
 * Created by jmgomez on 27/05/15.
 */
@RunWith(classOf[JUnitRunner])
class timerTest extends FlatSpec with Matchers {

  behavior of "timer"


  it should "return the correct time" in {
   implicit val log = mock(classOf[Logger])

    val timeInit = System.currentTimeMillis()
    timer.time("op")(operationTime)
    val timeEnd = System.currentTimeMillis()
    val logWritten = ArgumentCaptor.forClass(classOf[String]);

    verify(log).debug(logWritten.capture())
    logWritten.getValue should fullyMatch regex "millis: \\[\\d+\\] op"
  }

  it should "return the correct time2" in {
    implicit val log = mock(classOf[Logger])
    new ABSTimer{
    override def getUID[T]: String = {
        "op"
      }
    }.time(operationTime)


    val logWritten = ArgumentCaptor.forClass(classOf[String]);
    verify(log).debug(logWritten.capture())

    logWritten.getValue should fullyMatch regex "millis: \\[\\d+\\] op"
  }

  def operationTime: Unit ={
    Thread.sleep(100)
  }
}
