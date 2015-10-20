package com.danveloper.ratpack.graph.rendering

import com.danveloper.ratpack.graph.GraphModule
import com.danveloper.ratpack.graph.Node
import com.danveloper.ratpack.graph.NodeClassifier
import com.danveloper.ratpack.graph.NodeConverter
import com.danveloper.ratpack.graph.NodeProperties
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.collect.Sets
import ratpack.exec.Promise
import ratpack.func.Action
import ratpack.guice.Guice
import ratpack.test.embed.EmbeddedApp
import spock.lang.AutoCleanup
import spock.lang.Specification

class NodeRenderingSpec extends Specification {

  static NodeClassifier TEST_GEN = new NodeClassifier("test", "general")
  static Node NODE_1 = new Node(new NodeProperties("id1", TEST_GEN))
  static Node NODE_2 = new Node(new NodeProperties("id2", TEST_GEN))
  static Map NODE_DATA_1 = [foo: "bar"]
  static Map NODE_DATA_2 = [foo: "baz"]

  def mapper = new ObjectMapper()

  @AutoCleanup
  @Delegate
  EmbeddedApp app = EmbeddedApp.of({ spec ->
    spec.registry(Guice.registry { b ->
      b.module(GraphModule)
      b.bind(NodeConverter, TestNodeConverter)
    })
    .handlers { chain ->
      chain.get { ctx ->
        ctx.render NODE_1
      }
      chain.get("list") { ctx ->
        ctx.render(new NodeCollection(Sets.newLinkedHashSet([NODE_1, NODE_2])))
      }
    }
  } as Action)

  void "should be able to render a node and have it go through the converter"() {
    expect:
    map(httpClient.getText()) == NODE_DATA_1
  }

  void "should be able to render a collection of nodes and have each go through the converter"() {
    given:
    def l = list(httpClient.getText("list"))

    expect:
    2 == l.size()
    l[0] == NODE_DATA_1
    l[1] == NODE_DATA_2
  }

  private List list(String json) {
    mapper.readValue(json, List)
  }

  private Map map(String json) {
    mapper.readValue(json, Map)
  }

  static class TestNodeConverter implements NodeConverter<Map> {

    @Override
    NodeClassifier getClassifier() {
      TEST_GEN
    }

    @Override
    Promise<Map> convert(Node node) {
      def data = node.properties.id == "id1" ? NODE_DATA_1 : NODE_DATA_2
      Promise.value(data)
    }
  }
}
