package com.henryp.lambda.integration.utils

import kafka.serializer.{Decoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Duration, StreamingContext}

import scala.reflect.ClassTag

abstract class SparkStreamingRunning[K: ClassTag, V: ClassTag, KD <: Decoder[K] : ClassTag, VD <: Decoder[V] : ClassTag] extends MiniDfsClusterRunning with KafkaRunning {

  val checkpointFolder = hdfsUri + "checkpoint_directory"

  val streamingContext = StreamingContext.getOrCreate(checkpointFolder, creatingFunc(checkpointFolder))
  streamingContext.start()

  def creatingFunc(checkpointFolder: String): () => StreamingContext = () => {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("myAppName")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.ui.enabled", "false")
    val streamingContext = new StreamingContext(sparkConf, Duration(5000))
    streamingContext.checkpoint(checkpointFolder)

    println(s"PORTS: kafka = $kafkaPort, zookeeper = $zkPort")
    val dStream: InputDStream[(K, V)] =
      KafkaUtils.createDirectStream[K, V, KD, VD](
        ssc = streamingContext,
        kafkaParams = kafkaConf,
        topics = Set(topicName))

    handlerForPairs(checkpointFolder)(dStream)
    streamingContext
  }

  def handlerForPairs(path: String): (DStream[(K, V)]) => Unit

}
