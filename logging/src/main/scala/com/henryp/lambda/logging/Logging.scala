package com.henryp.lambda.logging

trait Logging {

  def info(msg: String): Unit = {
    println(msg)
  }

}
