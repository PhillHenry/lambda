package com.henryp.lambda.integration.utils

import java.net.ServerSocket

class PortUtils {


}

object PortUtils {
  def apply() = {
    val socket = new ServerSocket(0)
    val port = socket.getLocalPort
    socket.close()
    port
  }
}
