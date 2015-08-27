package com.henryp

import akka.actor.{PoisonPill, Props, ActorSystem}
import akka.cluster.routing.{ClusterRouterGroup, ClusterRouterGroupSettings, HeapMetricsSelector, AdaptiveLoadBalancingGroup}
import akka.contrib.pattern.ClusterSingletonManager
import akka.routing.RoundRobinPool
import com.henryp.actors.{SingletonActor, TickActor}
import com.typesafe.config.ConfigFactory

/**
 * mvn exec:java -Dexec.mainClass=com.henryp.ClusterApp -Dexec.args="9119 127.0.0.1"
 */
object ClusterApp extends App {

  val myPort = args(0)
  val myHost = args(1)

  val actorSystem = ActorSystem(systemName, ConfigFactory.parseString(
    s"""
        akka {
          actor.provider = "akka.cluster.ClusterActorRefProvider"

          remote.log-remote-lifecycle-events = on
          remote.netty.tcp.port = $myPort
          remote.netty.tcp.hostname = "127.0.0.1"

          cluster {
              seed-nodes = [
                  "akka.tcp://$systemName@$myHost:$seedPort",
                  "akka.tcp://$systemName@$myHost:9120",
              ]
              roles = [ "$role" ]
              role.broker.min-nr-of-members = 1
              akka.cluster.auto-down-unreachable-after = 10s
          }
        }
      """))

  // see http://www.warski.org/blog/2014/11/clustering-reactmq-with-akka-cluster/
  actorSystem.actorOf(ClusterSingletonManager.props(
    singletonProps = Props(classOf[SingletonActor]),
    singletonName = singletonName,
    terminationMessage = PoisonPill,
    role = Some(role)),
    name = nameOfSingletonActor)

  val localRouter =
    actorSystem.actorOf(Props(classOf[TickActor]).withRouter(RoundRobinPool(nrOfInstances = 2)), localSamplerActorName)

  if (myPort == seedPort)
  {
    val loadBalancingGroup = AdaptiveLoadBalancingGroup(HeapMetricsSelector)
    val clusterRouterGroupSettings = ClusterRouterGroupSettings(
      totalInstances = 3,
      routeesPaths = List(s"/user/$localSamplerActorName"),
      allowLocalRoutees = true,
      useRole = None
    )

    val clusterRouterGroup = ClusterRouterGroup(loadBalancingGroup, clusterRouterGroupSettings)
    val clusterActor = actorSystem.actorOf(clusterRouterGroup.props(), "clusterWideRouter")
  }
}
