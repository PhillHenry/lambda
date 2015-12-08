package com.henryp.lambda.spark.streaming.lmax

import java.util

import com.henryp.lambda.logging.Logging
import com.lmax.api.account.LoginRequest.ProductType._
import com.lmax.api.account.{LoginCallback, LoginRequest}
import com.lmax.api.orderbook._
import com.lmax.api.{Callback, FailureResponse, LmaxApi, Session}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future

object Lmax extends Logging {

  def listen(config: LmaxStreamingConfig, orderBookListener: OrderBookEventListener): List[Long] = {
    val loginCallback = new LoginClient(config.url, config.username, orderBookListener)

    login(config, loginCallback)

    startLmaxSessionNonBlocking(loginCallback.session)

    loginCallback.instruments.toList
  }

  def allInstruments(config: LmaxStreamingConfig): List[Long] = {
    val ids = new ArrayBuffer[Long]()
    login(config, new LoginCallback {
      override def onLoginFailure(failureResponse: FailureResponse): Unit = error(failureResponse.getDescription)

      override def onLoginSuccess(session: Session): Unit = {
        allInstruments(session).foreach(id => ids += id)
        session.stop()
      }
    })
    ids.toList
  }

  def login(config: LmaxStreamingConfig, loginCallback: LoginCallback): Unit = {
    val lmaxApi = new LmaxApi(config.url)
    val loginRequest = new LoginRequest(config.username, config.password, CFD_DEMO)
    lmaxApi.login(loginRequest, loginCallback)
  }

  def startLmaxSessionNonBlocking(lmaxSession: Session): Future[Unit] = {
    import scala.concurrent.ExecutionContext.Implicits._
    Future {
      info("starting LMAX")
      lmaxSession.start()
    }
  }

  def subscriptionCallback(instrumentId: Long): Callback = new Callback {

    override def onFailure(failureResponse: FailureResponse): Unit = error(failureResponse.getDescription)

    override def onSuccess(): Unit = info(s"Subscribed to $instrumentId")
  }

  /**
    * @see com.lmax.api.MarketDataClient
    */
  def allInstruments(lmaxSession: Session): Array[Long] = {
    import scala.collection.JavaConversions._

    val offset  = Array(0L)
    val hasMore = Array(true)
    val allInstruments = ArrayBuffer[Long]()

    while (hasMore(0)) {
      val searchInstrumentCallback = new SearchInstrumentCallback() {
        override def onSuccess(instruments: util.List[Instrument], hasMoreResults: Boolean): Unit = {
          hasMore(0) = hasMoreResults

          for (instrument <- instruments) {
            val instrumentId = instrument.getId
            allInstruments += instrumentId
            offset(0) = instrumentId
          }
        }

        override def onFailure(failureResponse: FailureResponse): Unit = {
          hasMore(0) = false
          throw new RuntimeException("Failed: " + failureResponse)
        }
      }
      lmaxSession.searchInstruments(new SearchInstrumentRequest("", offset(0)), searchInstrumentCallback)
    }

    allInstruments.toArray
  }

}
