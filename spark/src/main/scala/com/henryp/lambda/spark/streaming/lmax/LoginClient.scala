package com.henryp.lambda.spark.streaming.lmax

import com.henryp.lambda.logging.Logging
import com.henryp.lambda.spark.streaming.lmax.Lmax._
import com.lmax.api.account.LoginCallback
import com.lmax.api.orderbook._
import com.lmax.api.{FailureResponse, Session}
import org.apache.spark.streaming.receiver.Receiver

class LoginClient(url: String, username: String, listener: OrderBookEventListener)
  extends LoginCallback with Logging {

  var instruments: Array[Long] = null

  var receiver: Receiver[OrderBookEvent] = null

  var session: Session = null

  def setReceiver(orderBookReceiver: Receiver[OrderBookEvent]): Unit = receiver = orderBookReceiver

  override def onLoginFailure(failureResponse: FailureResponse): Unit = {
    error(s"Could not login to $url with username $username")
    receiver.stop(failureResponse.getDescription, failureResponse.getException)
  }

  override def onLoginSuccess(lmaxSession: Session): Unit = {
    info(s"Successfully logged in to LMAX ($url) as $username")

    lmaxSession.registerOrderBookEventListener(listener)

    instruments = allInstruments(lmaxSession)
    instruments.foreach(instrumentId => subscribeTo(lmaxSession, instrumentId))

    session = lmaxSession
  }

  def subscribeTo(lmaxSession: Session, instrumentId: Long): Unit = {
    lmaxSession.subscribe(new OrderBookSubscriptionRequest(instrumentId), subscriptionCallback(instrumentId))
  }
}
