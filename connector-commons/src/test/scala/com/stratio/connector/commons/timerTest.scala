package com.stratio.connector.commons

import java.util.UUID

import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.slf4j.Logger


import org.mockito.Mockito._

/**
 * Created by jmgomez on 27/05/15.
 */
class timerTest extends FlatSpec with Matchers {

  behavior of "timer"


  it should "return the correct time" in {
   implicit val log = mock(classOf[Logger])

    timer.time("op")(operationTime)
    verify(log).debug("[millis: 1000] op")
    
  }

  it should "return the correct time2" in {
    implicit val log = mock(classOf[Logger])

    new ABSTimer{
    override def getUID[T]: String = {
        "op"
      }
    }.time(operationTime)
    verify(log).debug("[millis: 1000] op ")

  }

  def operationTime: Unit ={
    Thread.sleep(1000)
  }
}
