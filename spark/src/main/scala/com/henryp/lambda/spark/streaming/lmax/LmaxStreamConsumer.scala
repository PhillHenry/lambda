package com.henryp.lambda.spark.streaming.lmax

import com.henryp.lambda.logging.Logging
import com.lmax.api.orderbook.{OrderBookEvent, PricePoint}
import org.apache.spark.streaming.dstream.DStream

class LmaxStreamConsumer extends (DStream[OrderBookEvent] => Unit) with Logging {

  import scala.collection.JavaConversions._

  case class InstrumentToTimeToPrices(id: Long, timeToPrices: TimeToPrices)

  case class TimeToPrices(timestamp: Long, prices: Seq[PricePoint])

  override def apply(stream: DStream[OrderBookEvent]): Unit = {
    stream.foreachRDD { rdd =>
      val count = rdd.count()
      if (count > 0) {
        println(s"RDD with $count elements") // TODO something useful
        val instrumentsToAsk = rdd.map(x => x.getInstrumentId -> TimeToPrices(x.getTimeStamp, x.getAskPrices)).groupByKey().cache()

        def recurse(toCompare: (Long, Iterable[TimeToPrices]), others: Seq[(Long, Iterable[TimeToPrices])]): Unit = {

        }
//         recurse(instrumentsToAsk.top(), null)
      }
    }
  }

}
