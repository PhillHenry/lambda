package com.henryp.lambda.integration.utils

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext

trait SparkStreamingRunning extends MiniDfsClusterRunning {

  val checkpointFolder = hdfsUri + "checkpoint_directory"

  val streamingContext = StreamingContext.getOrCreate(checkpointFolder, creatingFunc(checkpointFolder))
  streamingContext.start()

  def creatingFunc(checkpointFolder: String): () => StreamingContext

}
