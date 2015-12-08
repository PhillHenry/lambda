package com.henryp.lambda.spark.streaming.lmax

import com.henryp.lambda.logging.Logging
import com.henryp.lambda.spark.streaming.lmax.Lmax.listen
import com.lmax.api.orderbook._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

/**
  * Must be serializable.
  */
class MarketDataReceiver(config: LmaxStreamingConfig)
  extends Receiver[OrderBookEvent](StorageLevel.MEMORY_AND_DISK) with Logging with OrderBookEventListener {

  @volatile var isRunning = false

  override def onStart(): Unit = {
    listen(config, this)
    isRunning = true
  }

  override def notify(orderBookEvent: OrderBookEvent): Unit = {
    if (isRunning) {
      store(orderBookEvent)
    }
  }

  override def onStop(): Unit = info("Stopping the LMAX receiver")
}