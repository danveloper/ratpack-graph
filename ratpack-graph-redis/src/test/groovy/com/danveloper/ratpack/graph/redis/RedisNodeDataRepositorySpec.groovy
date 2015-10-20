package com.danveloper.ratpack.graph.redis

import com.danveloper.ratpack.graph.NodeProperties
import com.fasterxml.jackson.databind.ObjectMapper
import spock.lang.Shared

class RedisNodeDataRepositorySpec extends RedisRepositorySpec {

  @Shared
  def repo = new RedisNodeDataRepository(new RedisGraphModule.Config(port: port), new ObjectMapper())

  def setupSpec() {
    repo.onStart(null)
  }

  void "should be able to store objects"() {
    setup:
    def props = new NodeProperties("id1", TEST_GEN)
    def testObj = new TestObject(foo: "bar")

    when:
    execControl.executeSingle { repo.save(props, testObj) }

    and:
    def upd = execControl.yieldSingle { repo.<TestObject>get(props) }.valueOrThrow

    then:
    upd.foo == testObj.foo
  }

  static class TestObject {
    String foo
  }
}
