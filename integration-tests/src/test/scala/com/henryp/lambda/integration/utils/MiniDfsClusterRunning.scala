package com.henryp.lambda.integration.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.MiniDFSCluster
import com.henryp.thirdparty.kafka.tmpDirectory

trait MiniDfsClusterRunning {

  val baseDir = tmpDirectory("tests").getAbsoluteFile
  val conf = new Configuration()
  conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath)
  val builder = new MiniDFSCluster.Builder(conf)

  println("Attempting to start HDFS")
  val hdfsCluster = builder.build()
  val distributedFS = hdfsCluster.getFileSystem
  val hdfsUri = "hdfs://127.0.0.1:" + hdfsCluster.getNameNodePort + "/"

}
