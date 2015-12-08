package com.henryp.lambda.logging

trait Logging {

  def info(msg: String): Unit = output(msg) // TODO - some proper logging

  def error(msg: String): Unit = output(msg) // TODO - some proper logging

  def debug(msg: String): Unit = output(msg) // TODO proper debugging

  def output(msg: String) = println(Thread.currentThread().getName + ": " + msg)

}
