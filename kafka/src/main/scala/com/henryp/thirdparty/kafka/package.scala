package com.henryp.thirdparty

import java.io.File
import java.nio.file.Files

import org.apache.commons.io.FileUtils


package object kafka {

  def tmpDirectory(path: String): File = {
    val file = Files.createTempDirectory(path).toFile
    FileUtils.forceDeleteOnExit(file)
    file
  }

}
