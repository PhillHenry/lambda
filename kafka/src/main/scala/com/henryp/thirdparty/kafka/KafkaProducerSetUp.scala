package com.henryp.thirdparty.kafka

import java.util.Properties

import kafka.producer.{Producer, ProducerConfig}


object KafkaProducerSetUp {

  def apply[K, V](props: Properties): Producer[K, V] = {
    val config = new ProducerConfig(props)
    new Producer[K, V](config)
  }

}
