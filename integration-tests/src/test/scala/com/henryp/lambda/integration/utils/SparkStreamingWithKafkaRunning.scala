package com.henryp.lambda.integration.utils

import com.henryp.lambda.logging.Logging
import kafka.serializer.Decoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Duration, StreamingContext}

import scala.reflect.ClassTag

abstract class SparkStreamingWithKafkaRunning[K: ClassTag, V: ClassTag, KD <: Decoder[K] : ClassTag, VD <: Decoder[V] : ClassTag]
  extends MiniDfsClusterRunning with KafkaRunning with Logging {

  val batchDuration = Duration(1000)
  val checkPointRelativeFolder = "checkpoint_directory"
  val checkpointFolder = hdfsUri + checkPointRelativeFolder
  val streamingContext = StreamingContext.getOrCreate(checkpointFolder, creatingFunc(checkpointFolder))
  streamingContext.start()

  def creatingFunc(checkpointFolder: String): () => StreamingContext = () => {
    val sparkConf = new SparkConf()
      .setMaster("local[*]") // TODO make configurable
      .setAppName("myAppName")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.ui.enabled", "false")
    val streamingContext = new StreamingContext(sparkConf, batchDuration)
    streamingContext.checkpoint(checkpointFolder)

    info(s"PORTS: kafka = $kafkaPort, zookeeper = $zkPort")
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
