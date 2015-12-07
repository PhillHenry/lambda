package com.henryp.lambda.spark.streaming.lmax

import java.util

import com.henryp.lambda.logging.Logging
import com.henryp.lambda.spark.streaming.lmax.MarketDataReceiver.loginClient
import com.lmax.api.account.LoginRequest.ProductType
import com.lmax.api.account.{LoginCallback, LoginRequest}
import com.lmax.api.orderbook._
import com.lmax.api.{Callback, FailureResponse, LmaxApi, Session}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import scala.concurrent.Future

class MarketDataReceiver(url: String, username: String, password: String, productType: ProductType)
  extends Receiver[OrderBookEvent](StorageLevel.MEMORY_AND_DISK) with Logging {

  info(s"url = $url, username = $username, ${password.map(x => '*')}")

  override def onStart(): Unit = {
    info(s"Starting receiver against URL $url, username $username")
    val lmaxApi       = createLmaxApi()
    val loginRequest  = new LoginRequest(username, password, productType)

    import scala.concurrent.ExecutionContext.Implicits._
    Future {
      lmaxApi.login(loginRequest, loginClient(url, username, this))
    }
  }

  def createLmaxApi(): LmaxApi = new LmaxApi(url)

  override def onStop(): Unit = info("Stopping the LMAX receiver")
}

object MarketDataReceiver {
  def loginClient(url: String, username: String, receiver: Receiver[OrderBookEvent])
    = new OrderBookEventListener with LoginCallback with Logging {
    override def onLoginFailure(failureResponse: FailureResponse): Unit = {
      error(s"Could not login to $url with username $username")
      receiver.stop(failureResponse.getDescription, failureResponse.getException)
    }

    override def onLoginSuccess(session: Session): Unit = {
      info(s"Successfully logged in to LMAX ($url) as $username")

      subscribeToAll(session)

      session.start()
    }

    def subscriptionCallback(instrumentId: Long): Callback = new Callback {
      override def onFailure(failureResponse: FailureResponse): Unit = error(failureResponse.getDescription)

      override def onSuccess(): Unit = info(s"Subscribed to $instrumentId")
    }

    /**
      * @see com.lmax.api.MarketDataClient
      */
    def subscribeToAll(session: Session): Unit = {
      import scala.collection.JavaConversions._

      session.registerOrderBookEventListener(this)

      val offset  = Array(0L)
      val hasMore = Array(true)

      while (hasMore(0)) {
        val searchInstrumentCallback = new SearchInstrumentCallback() {
          override def onSuccess(instruments: util.List[Instrument], hasMoreResults: Boolean): Unit = {
            hasMore(0) = hasMoreResults

            for (instrument <- instruments) {
              val instrumentId = instrument.getId
              info(s"Instrument: $instrumentId,  + ${instrument.getName}")
              session.subscribe(new OrderBookSubscriptionRequest(instrumentId), subscriptionCallback(instrumentId))
              offset(0) = instrumentId
            }
          }

          override def onFailure(failureResponse: FailureResponse): Unit = {
            hasMore(0) = false
            throw new RuntimeException("Failed: " + failureResponse)
          }
        }
        session.searchInstruments(new SearchInstrumentRequest("", offset(0)), searchInstrumentCallback)
      }
    }


    override def notify(orderBookEvent: OrderBookEvent): Unit = {
      info("order book event = " + orderBookEvent.toString)
      receiver.store(orderBookEvent)
    }
  }

}