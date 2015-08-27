package com.henryp

import akka.actor.{Props, ActorSystem}
import akka.contrib.pattern.ClusterSingletonProxy
import com.henryp._
import com.henryp.actors.SingletonActor
import com.typesafe.config.ConfigFactory

/**
 * http://doc.akka.io/docs/akka/current/contrib/cluster-singleton.html
 * http://stackoverflow.com/questions/17233653/akka-singleton-not-accepting-messages
 *
 * mvn exec:java -Dexec.mainClass=com.henryp.ClusterClient -Dexec.args="9119 127.0.0.1"
 */
object ClusterClient extends App {
  val remotePort = args(0)
  val remoteHost = args(1)

  val actorSystem = ActorSystem(systemName, ConfigFactory.parseString(
    s"""
       akka {
         actor.provider = "akka.cluster.ClusterActorRefProvider"

         remote.netty.tcp.port = 0
         remote.netty.tcp.hostname = "127.0.0.1"

         cluster {
           seed-nodes = [
             "akka.tcp://$systemName@$remoteHost:$remotePort",
             "akka.tcp://$systemName@$remoteHost:9120"
             ]
             auto-down-unreachable-after = 10s
         }
       }
      """))



  val clusterSingletonProxy = actorSystem.actorOf(ClusterSingletonProxy.props(
      singletonPath = s"/user/$nameOfSingletonActor/$singletonName",
      role = Some(role)
    ),
    name = s"$singletonName-proxy"
  )

  while (true) {
    clusterSingletonProxy ! "hello"
    Thread.sleep(100)
    print(".")
  }
}
