package com.henryp.lambda.spark.streaming.lmax

import com.henryp.lambda.logging.Logging
import com.lmax.api.account.LoginRequest.ProductType
import com.lmax.api.orderbook.{OrderBookEvent, OrderBookEventListener}
import com.lmax.api.{FailureResponse, Session, LmaxApi}
import com.lmax.api.account.{LoginCallback, LoginRequest}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import scala.concurrent.Future

class MarketDataReceiver(url: String, username: String, password: String, productType: String)
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK) with Logging {

  val loginClient = new OrderBookEventListener with LoginCallback with Logging {
    override def onLoginFailure(failureResponse: FailureResponse): Unit = error(s"Could not login to $url with username $username")

    override def onLoginSuccess(session: Session): Unit = info("Logged in to LMAX")

    override def notify(orderBookEvent: OrderBookEvent): Unit = {
      val orderBookInfo = orderBookEvent.toString
      info(orderBookInfo)
      store(orderBookInfo)
    }
  }

  override def onStart(): Unit = {
    val lmaxApi = new LmaxApi(url)
    val loginRequest = new LoginRequest(username, password, ProductType.valueOf(productType))

    import scala.concurrent.ExecutionContext.Implicits._
    Future {
      lmaxApi.login(loginRequest, loginClient)
    }

  }

  override def onStop(): Unit = info("Stopping the LMAX receiver")
}
