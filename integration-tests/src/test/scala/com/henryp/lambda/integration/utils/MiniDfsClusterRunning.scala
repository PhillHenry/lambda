package com.henryp.lambda.integration.utils

import com.henryp.lambda.logging.Logging
import com.henryp.thirdparty.kafka.tmpDirectory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.MiniDFSCluster

import scala.collection.mutable.ArrayBuffer

trait MiniDfsClusterRunning extends Logging {

  val baseDir = tmpDirectory("tests").getAbsoluteFile
  val conf = new Configuration()
  conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath)
  val builder = new MiniDFSCluster.Builder(conf)

  info("Attempting to start HDFS")
  val hdfsCluster = builder.build()
  val distributedFS = hdfsCluster.getFileSystem
  val hdfsUri = "hdfs://127.0.0.1:" + hdfsCluster.getNameNodePort + "/"

  def list(path: String): List[Path] = {
    info(s"Looking in $path")

    val files = distributedFS.listFiles(new Path(path), true)

    val allPaths = ArrayBuffer[Path]()
    while (files.hasNext) {
      val file = files.next
      allPaths += file.getPath
    }

    allPaths.toList
  }

}
