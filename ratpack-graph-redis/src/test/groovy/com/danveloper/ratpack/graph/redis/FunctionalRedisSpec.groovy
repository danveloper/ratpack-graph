package com.danveloper.ratpack.graph.redis

import com.danveloper.ratpack.graph.NodeClassifier
import com.danveloper.ratpack.graph.NodeConverter
import com.danveloper.ratpack.graph.NodeDataRepository
import com.danveloper.ratpack.graph.NodeProperties
import com.danveloper.ratpack.graph.NodeRepository
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.inject.Inject
import ratpack.exec.Promise
import ratpack.func.Action
import ratpack.guice.Guice
import ratpack.test.embed.EmbeddedApp
import spock.lang.AutoCleanup
import com.danveloper.ratpack.graph.Node

class FunctionalRedisSpec extends RedisRepositorySpec {

  static Node NODE_1 = new Node(new NodeProperties("id1", TEST_GEN))
  static TestObj NODE_1_DATA = new TestObj(foo: "bar")

  def mapper = new ObjectMapper()

  @Delegate
  @AutoCleanup
  EmbeddedApp app = EmbeddedApp.of({ spec ->
    spec.registry(Guice.registry { b ->
      b.module(RedisGraphModule) { c -> c.port = port }
      b.bind(NodeConverter, TestConverter)
    })
    .handlers { chain ->
      chain.get { ctx ->
        NodeRepository nodeRepository = ctx.get(NodeRepository)
        NodeDataRepository nodeDataRepository = ctx.get(NodeDataRepository)
        nodeRepository.save(NODE_1).flatMap {
          nodeDataRepository.save(NODE_1.properties, NODE_1_DATA).promise()
        }.then {
          ctx.render NODE_1
        }
      }
    }
  } as Action)

  void "should store, save, and convert nodes and node data"() {
    given:
    def resp = mapper.readValue(httpClient.getText(), Map)

    expect:
    resp.foo == NODE_1_DATA.foo
  }

  static class TestObj {
    String foo
  }

  static class TestConverter implements NodeConverter<TestObj> {
    @Inject
    NodeDataRepository nodeDataRepository

    @Override
    NodeClassifier getClassifier() {
      TEST_GEN
    }

    @Override
    Promise<TestObj> convert(Node node) {
      nodeDataRepository.<TestObj>get(node.properties)
    }
  }
}
