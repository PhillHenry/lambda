package com.henryp.lambda.spark.streaming.lmax

import com.lmax.api.orderbook.OrderBookEvent
import org.apache.spark.streaming.dstream.DStream

class LmaxStreamConsumer extends (DStream[OrderBookEvent] => Unit) {

  override def apply(stream: DStream[OrderBookEvent]): Unit = {
    stream.foreachRDD { rdd =>
      if (rdd.count() > 0) {
        ??? // TODO something useful
      }
    }
  }

}
