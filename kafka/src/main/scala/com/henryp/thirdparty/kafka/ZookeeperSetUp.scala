package com.henryp.thirdparty.kafka

import java.net.InetSocketAddress

import org.apache.zookeeper.server.{ServerCnxnFactory, ZooKeeperServer}


class ZookeeperStarter() {

  def startZookeeper(localhost: String, zkPort: Int): ServerCnxnFactory = {
    val zookeeper = new ZooKeeperServer(tmpDirectory("zkShapshotDir"), tmpDirectory("zkLogs"), 1000)
    val zk = ServerCnxnFactory.createFactory(new InetSocketAddress(localhost, zkPort), 10)
    zk.startup(zookeeper)
    zk
  }

}

object ZookeeperSetUp {

  def apply(hostname: String, port: Int): ServerCnxnFactory = {
    new ZookeeperStarter().startZookeeper(hostname, port)
  }
  
}
