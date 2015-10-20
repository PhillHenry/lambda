package com.henryp.lambda.integration.utils

import java.util.Properties

import com.henryp.thirdparty.kafka.KafkaSetUp

trait KafkaRunning extends ZookeeperRunning {

  val kafkaPort = PortUtils()

  val topicName = "topicName"

  val kafkaConf: Map[String, String] = KafkaSetUp.kafkaConf(hostname, kafkaPort)

  val kafkaProperties = new Properties
  kafkaConf map { case (k, v) => kafkaProperties.put(k, v) }

  val kafkaEndpoint = KafkaSetUp.toLocalEndPoint(hostname, kafkaPort)

  println("Attempting to start Kafka")
  val kafka = KafkaSetUp(hostname, kafkaPort, zkPort, topicName)


}
