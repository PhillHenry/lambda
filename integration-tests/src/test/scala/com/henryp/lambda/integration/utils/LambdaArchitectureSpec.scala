package com.henryp.lambda.integration.utils

import java.util.Properties
import java.util.concurrent.{TimeUnit, CountDownLatch}

import com.henryp.thirdparty.kafka.KafkaProducerSetUp
import kafka.producer.KeyedMessage
import kafka.serializer.StringDecoder
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.OutputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.streaming.dstream.DStream
import org.scalatest.{Matchers, WordSpec}

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class LambdaArchitectureSpec extends WordSpec with Matchers {

  "the stack" should {
    "stream" in new SparkStreamingRunning[String, String, StringDecoder, StringDecoder] {

      override def handlerForPairs(path: String): (DStream[(String, String)]) => Unit = {
        LambdaArchitectureSpec.consumingFn(path)
      }

      val filesBefore = list("/")

      withClue(filesBefore.mkString(" ")) {
        filesBefore should have size 0
      }

      val producer = KafkaProducerSetUp[Array[Byte], Array[Byte]](kafkaProperties)
      val message = new KeyedMessage(topicName, "key".getBytes, "value".getBytes)
      producer.send(message)

      Thread.sleep((batchDuration.milliseconds * 2).toInt)

      list("/") should not be empty
    }
  }

}

object LambdaArchitectureSpec {
  def consumingFn(path: String): (DStream[(String, String)]) => Unit = {
    stream =>
      stream.foreachRDD { rdd =>
        if (rdd.count() > 0) {
          println("RDD count = " + rdd.count())
          val jobConf = new JobConf()
          val clazz: Class[_ <: OutputFormat[_, _]] = classOf[TextOutputFormat[String, String]]
          // do what you need to do here.
          // After, we save the data to HDFS so:
          rdd.saveAsNewAPIHadoopFile(
            path,
            classOf[String],
            classOf[String],
            clazz,
            jobConf)
        }
      }
  }
}
