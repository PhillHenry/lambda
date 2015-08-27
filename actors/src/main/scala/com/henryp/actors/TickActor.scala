package com.henryp.actors

import akka.actor.{ActorLogging, Actor}
import akka.actor.Actor.Receive

class TickActor extends Actor with ActorLogging {

  var count = 0

  override def receive: Receive = {
    case _ => count = count + 1
  }
}
