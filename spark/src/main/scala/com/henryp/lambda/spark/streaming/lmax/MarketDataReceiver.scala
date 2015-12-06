package com.henryp.lambda.spark.streaming.lmax

import com.henryp.lambda.logging.Logging
import com.lmax.api.LmaxApi
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

class MarketDataReceiver(url: String, username: String, password: String, productType: String)
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK) with Logging {

  override def onStart(): Unit = {
    val lmaxApi = new LmaxApi(url)
    ???
  }

  override def onStop(): Unit = ???
}
