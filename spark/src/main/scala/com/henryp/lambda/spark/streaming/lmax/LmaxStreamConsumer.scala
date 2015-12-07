package com.henryp.lambda.spark.streaming.lmax

import com.henryp.lambda.logging.Logging
import com.lmax.api.orderbook.OrderBookEvent
import org.apache.spark.streaming.dstream.DStream

class LmaxStreamConsumer extends (DStream[OrderBookEvent] => Unit) with Logging {

  override def apply(stream: DStream[OrderBookEvent]): Unit = {
    stream.foreachRDD { rdd =>
      val count = rdd.count()
      if (count > 0) {
        println(s"RDD with $count elements") // TODO something useful
      }
    }
  }

}
