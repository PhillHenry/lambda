package com.henryp.lambda.integration.utils

import kafka.serializer.StringDecoder
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.OutputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.streaming.dstream.DStream
import org.scalatest.{Matchers, WordSpec}

class LambdaArchitectureSpec extends WordSpec with Matchers {

  "the stack" should {
    "stream" in new SparkStreamingRunning[String, String, StringDecoder, StringDecoder] {

      override def handlerForPairs(path: String): (DStream[(String, String)]) => Unit = {
        stream =>
          stream.foreachRDD { rdd =>
            if (rdd.count() > 0) {
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
  }

}
