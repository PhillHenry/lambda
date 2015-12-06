package com.henryp.actors

import akka.actor.{Actor, ActorLogging}

class TickActor extends Actor with ActorLogging {

  var count = 0

  override def receive: Receive = {
    case _ => count = count + 1
  }
}
