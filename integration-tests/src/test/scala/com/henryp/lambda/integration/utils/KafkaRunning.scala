package com.henryp.lambda.integration.utils

import java.util.Properties

import com.henryp.lambda.logging.Logging
import com.henryp.thirdparty.kafka.KafkaSetUp

trait KafkaRunning extends ZookeeperRunning with Logging {

  val kafkaPort = PortUtils()

  val topicName = "topicName"

  val kafkaConf: Map[String, String] = KafkaSetUp.kafkaConf(hostname, kafkaPort)

  val kafkaProperties = new Properties
  kafkaConf foreach { case (k, v) => kafkaProperties.put(k, v) }

  val kafkaEndpoint = KafkaSetUp.toLocalEndPoint(hostname, kafkaPort)

  info("Attempting to start Kafka")
  val kafka = KafkaSetUp(hostname, kafkaPort, zkPort, topicName)


}
