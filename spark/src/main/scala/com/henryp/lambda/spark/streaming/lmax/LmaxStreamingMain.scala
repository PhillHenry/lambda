package com.henryp.lambda.spark.streaming.lmax

import com.henryp.lambda.logging.Logging
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
    start(parseArgs(args))

    Thread.sleep(20000) // TODO sleep forever when testing is finished
  }

  def start(configOpt: Option[LmaxStreamingConfig]): Unit = {
    configOpt.foreach { config =>
      info(s"url = ${config.url}, username = ${config.username}, ${config.password.map(x => '*')}")

      val context           = getSparkContext(config)
      val streamFn          = initConsumer(config, context)

      startStreaming(config, context, streamFn)
    }
  }

  def startStreaming(config: LmaxStreamingConfig, context: SparkContext, streamFn: LmaxStreamConsumer): Unit = {
    val ssc       = initStreamingContext(config, context)
    val receiver  = new MarketDataReceiver(config)
    val dStream   = ssc.receiverStream(receiver)
    streamFn(dStream)
    info("starting ssc")
    ssc.start()
  }

  def initStreamingContext(config: LmaxStreamingConfig, context: SparkContext): StreamingContext = {
    val ssc = new StreamingContext(context, Duration(10000))
    ssc.checkpoint(config.directory)
    ssc
  }

  def initConsumer(config: LmaxStreamingConfig, context: SparkContext): LmaxStreamConsumer = {
    val instruments = Lmax.allInstruments(config)
    val bInstruments = context.broadcast(instruments)
    val streamFn = new LmaxStreamConsumer(bInstruments)
    streamFn
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
