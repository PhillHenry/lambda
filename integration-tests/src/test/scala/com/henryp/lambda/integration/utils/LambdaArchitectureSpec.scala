package com.henryp.lambda.integration.utils

import kafka.serializer.StringDecoder
import org.apache.hadoop.mapred.{JobConf}
import org.apache.hadoop.mapreduce.OutputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.scalatest.{Matchers, WordSpec}
import parquet.avro.AvroWriteSupport
import parquet.hadoop.ParquetOutputFormat

class LambdaArchitectureSpec extends WordSpec with Matchers {

  "the stack" should {
    "stream" in new ApplicationStack {

      type KeyType = String
      type ValueType = String
      type RddKeyValue = (KeyType, ValueType)

      override def creatingFunc(checkpointFolder: String): () => StreamingContext = () => {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("myAppName")
          .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .set("spark.ui.enabled", "false")
        val streamingContext = new StreamingContext(sparkConf, Duration(5000))
        streamingContext.checkpoint(checkpointFolder)

        println(s"PORTS: kafka = $kafkaPort, zookeeper = $zkPort")
        val dStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streamingContext,
        kafkaConf,
        Set(topicName))

        handlerForPairs(checkpointFolder)(dStream)
        streamingContext
      }

      def handlerForPairs(path: String): (DStream[(KeyType, ValueType)]) => Unit = {
        stream =>
          stream.foreachRDD { rdd =>
            if (rdd.count() > 0) {
              val jobConf = new JobConf()
              val clazz: Class[_ <: OutputFormat[_, _]] = classOf[TextOutputFormat[KeyType, ValueType]]
              // do what you need to do here.
              // After, we save the data to HDFS so:
              rdd.saveAsNewAPIHadoopFile(
                path,
                classOf[KeyType],
                classOf[ValueType],
                clazz,
                jobConf)
            }
          }
      }
    }
  }

}
