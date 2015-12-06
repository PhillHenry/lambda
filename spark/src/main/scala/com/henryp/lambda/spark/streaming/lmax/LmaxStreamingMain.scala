package com.henryp.lambda.spark.streaming.lmax

import com.henryp.lambda.logging.Logging
import com.lmax.api.account.LoginRequest.ProductType.CFD_DEMO
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

case class LmaxStreamingConfig(url: String = "https://web-order.london-demo.lmax.com",
                               username: String = "phillhenry",
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
    parseArgs(args).foreach { config =>
      val receiver = new MarketDataReceiver(config.url, config.username, config.password, CFD_DEMO)
      val ssc = new StreamingContext(getSparkContext(config), Duration(10000))
      ssc.checkpoint(config.directory)
      val dStream = ssc.receiverStream(receiver)
    }
  }

  def getSparkContext(config: LmaxStreamingConfig): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster(config.sparkUrl)
    conf.setAppName("ftse")
    val context = SparkContext.getOrCreate(conf)
    config.jars.foreach { jar =>
      debug(s"Adding JAR $jar")
      context.addJar(jar)
    }
    context
  }


}
