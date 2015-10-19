package com.henryp.lambda.integration.utils

import com.henryp.thirdparty.kafka.ZookeeperSetUp

trait ZookeeperRunning {

  val zkPort = PortUtils()

  val hostname = "localhost"

  println("Attempting to start Zookeeper")
  val zookeeper = ZookeeperSetUp(hostname, zkPort)

}
