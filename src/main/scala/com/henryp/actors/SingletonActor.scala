package com.henryp.actors

import akka.actor.{ActorLogging, Actor}

class SingletonActor extends Actor with ActorLogging {

  var counts = 0

  override def receive: Receive = {
    case x: String => {
      counts = counts + 1
      if (counts % 10 == 0) println(s"singleton counter = $counts")
    }
  }
}
