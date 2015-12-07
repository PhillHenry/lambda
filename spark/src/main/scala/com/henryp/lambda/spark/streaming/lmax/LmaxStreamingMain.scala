package com.henryp.lambda.spark.streaming.lmax

import com.henryp.lambda.logging.Logging
import com.lmax.api.account.LoginRequest.ProductType.CFD_DEMO
import com.lmax.api.orderbook.OrderBookEvent
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

case class LmaxStreamingConfig(url: String = "https://web-order.london-demo.lmax.com",
                               username: String = "philliphenry",
                               password: String = "secret",
                               sparkUrl: String = "local[*]",
                               directory: String = "/tmp/TODO",
                               jars: Seq[String] = Nil)

object LmaxStreamingMain extends Logging {

  def parseArgs(args: Array[String]): Option[LmaxStreamingConfig] = {
    val parser = new scopt.OptionParser[LmaxStreamingConfig]("StockCorrelation") {
      opt[String]('l', "url") action { case(value, config) => config.copy(url = value) } text "LMAX API URL"
      opt[String]('d', "directory") action { case(value, config) => config.copy(directory = value) } text "data directory"
      opt[String]('u', "username") action { case(value, config) => config.copy(username = value) } text "LMAX username"
      opt[String]('p', "password") action { case(value, config) => config.copy(password = value) } text "LMAX password"
      opt[String]('s', "spark") action { case(value, config) => config.copy(sparkUrl = value) } text "Spark URL"
      opt[Seq[String]]('j', "jars") valueName "<jar1>,<jar2>..."  action { (value, config) =>
        config.copy(jars = value)
      } text "jars"
    }
    parser.parse(args, LmaxStreamingConfig())
  }

  def main(args: Array[String]): Unit = {
    val dStreamOpt  = createStreamAndContext(parseArgs(args))
    val streamFn    = new LmaxStreamConsumer
    dStreamOpt.foreach { streamAndCtx =>
      val stream  = streamAndCtx._1
      val ssc     = streamAndCtx._2
      streamFn(stream)
      ssc.start()
    }
    Thread.sleep(20000)
  }

  def createStreamAndContext(configOpt: Option[LmaxStreamingConfig]): Option[(ReceiverInputDStream[OrderBookEvent], StreamingContext)] = {
    configOpt.map { config =>
      val receiver  = new MarketDataReceiver(config.url, config.username, config.password, CFD_DEMO)
      val ssc       = new StreamingContext(getSparkContext(config), Duration(10000))
      ssc.checkpoint(config.directory)
      val dStream   = ssc.receiverStream(receiver)
      (dStream, ssc)
    }
  }

  def getSparkContext(config: LmaxStreamingConfig): SparkContext = {
    val conf    = new SparkConf()
    conf.setMaster(config.sparkUrl)
    conf.setAppName("LMAX")
    val context = SparkContext.getOrCreate(conf)
    config.jars.foreach { jar =>
      debug(s"Adding JAR $jar")
      context.addJar(jar)
    }
    context
  }
}
