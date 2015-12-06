package com.henryp.lambda.spark.streaming.lmax

import com.henryp.lambda.logging.Logging
import com.lmax.api.account.LoginRequest.ProductType
import com.lmax.api.account.{LoginCallback, LoginRequest}
import com.lmax.api.orderbook.{OrderBookEvent, OrderBookEventListener}
import com.lmax.api.{FailureResponse, LmaxApi, Session}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import scala.concurrent.Future

class MarketDataReceiver(url: String, username: String, password: String, productType: ProductType)
  extends Receiver[OrderBookEvent](StorageLevel.MEMORY_AND_DISK) with Logging {

  val loginClient = new OrderBookEventListener with LoginCallback with Logging {
    override def onLoginFailure(failureResponse: FailureResponse): Unit = {
      error(s"Could not login to $url with username $username")
      stop(failureResponse.getDescription, failureResponse.getException)
    }

    override def onLoginSuccess(session: Session): Unit = info("Logged in to LMAX")

    override def notify(orderBookEvent: OrderBookEvent): Unit = {
      info(orderBookEvent.toString)
      store(orderBookEvent)
    }
  }

  override def onStart(): Unit = {
    val lmaxApi       = createLmaxApi()
    val loginRequest  = new LoginRequest(username, password, productType)

    import scala.concurrent.ExecutionContext.Implicits._
    Future {
      lmaxApi.login(loginRequest, loginClient)
    }
  }

  def createLmaxApi(): LmaxApi = new LmaxApi(url)

  override def onStop(): Unit = info("Stopping the LMAX receiver")
}
