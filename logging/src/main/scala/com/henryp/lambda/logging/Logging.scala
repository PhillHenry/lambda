package com.henryp.lambda.logging

trait Logging {

  def info(msg: String): Unit = {
    println(msg) // TODO - some proper logging
  }

  def error(msg: String): Unit = {
    println(msg) // TODO - some proper logging
  }

}
