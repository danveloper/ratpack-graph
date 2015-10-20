package com.danveloper.ratpack.graph.redis

class PortFinder {
  static int nextFree() {
    def ss = new ServerSocket()
    ss.bind(new InetSocketAddress(0))
    def port = ss.localPort
    ss.close()
    port
  }
}
