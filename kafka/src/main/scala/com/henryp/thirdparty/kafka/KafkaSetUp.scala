package com.henryp.thirdparty.kafka

import java.util.Properties

import com.henryp.thirdparty.kafka.KafkaSetUp.toLocalEndPoint
import kafka.admin.AdminUtils
import kafka.server.{KafkaConfig, KafkaServerStartable}
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient


class KafkaStarter(localhost: String, kPort: Int, zkPort: Int, topicName: String) {
  def startKafka(): KafkaServerStartable = {
    val props = new Properties()
    props.put("zookeeper.connect", toLocalEndPoint(localhost, zkPort))
    props.put("port", kPort.toString)
    props.put("broker.id", "0")
    props.put("log.dir", tmpDirectory("zkLogs").getAbsolutePath)
    props.put("num.partitions", "1")
    val server = new KafkaServerStartable(new KafkaConfig(props))
    server.startup()
    val zkClient = new ZkClient(toLocalEndPoint(localhost, zkPort), 3000, 3000, ZKStringSerializer)
    AdminUtils.createTopic(zkClient, topicName, 1, 1, new Properties)
    zkClient.close()
    server
  }
}

object KafkaSetUp {

  def apply(hostname: String, kPort: Int, zkPort: Int, topicName: String): KafkaServerStartable = {
    new KafkaStarter(hostname, kPort, zkPort, topicName).startKafka()
  }

  def kafkaConf(hostname: String, zkPort: Int): Map[String, String] = Map(
    "auto.offset.reset" -> "smallest",
    "metadata.broker.list" -> toLocalEndPoint(hostname, zkPort))


  def toLocalEndPoint(hostname: String, port: Int) = s"$hostname:$port"

}
