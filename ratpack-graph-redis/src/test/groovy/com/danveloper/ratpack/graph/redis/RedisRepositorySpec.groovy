package com.danveloper.ratpack.graph.redis

import com.danveloper.ratpack.graph.NodeClassifier
import com.lambdaworks.redis.RedisClient
import com.lambdaworks.redis.RedisConnection
import ratpack.test.exec.ExecHarness
import redis.embedded.RedisServer
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

import static com.danveloper.ratpack.graph.redis.PortFinder.nextFree

class RedisRepositorySpec extends Specification {
  static int port = nextFree()

  static NodeClassifier TEST_GEN = new NodeClassifier("test", "general")

  @AutoCleanup("stop")
  @Shared
  RedisServer redisServer = new RedisServer(port)

  @AutoCleanup
  ExecHarness execControl = ExecHarness.harness()

  def setupSpec() {
    redisServer.start()
  }

  def cleanup() {
    RedisConnection<String, String> conn = new RedisClient("localhost", port).connect()
    conn.flushall()
  }
}
