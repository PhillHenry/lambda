package com.henryp.lambda.integration.utils

import com.henryp.lambda.logging.Logging
import com.henryp.thirdparty.kafka.ZookeeperSetUp

trait ZookeeperRunning extends Logging {

  val zkPort = PortUtils()

  val hostname = "localhost"

  info("Attempting to start Zookeeper")
  val zookeeper = ZookeeperSetUp(hostname, zkPort)

}
