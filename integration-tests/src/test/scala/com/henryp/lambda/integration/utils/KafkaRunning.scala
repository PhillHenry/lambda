package com.henryp.lambda.integration.utils

import com.henryp.thirdparty.kafka.KafkaSetUp

trait KafkaRunning extends ZookeeperRunning {

  val kafkaPort = PortUtils()

  val topicName = "topicName"

  val kafkaConf: Map[String, String] = KafkaSetUp.kafkaConf(hostname, kafkaPort)

  println("Attempting to start Kafka")
  val kafka = KafkaSetUp(hostname, kafkaPort, zkPort, topicName)


}
