package com.danveloper.ratpack.graph

import com.danveloper.ratpack.graph.internal.InMemoryNodeRepository
import ratpack.test.exec.ExecHarness
import spock.lang.AutoCleanup
import spock.lang.Specification

class NodeRepositorySpec extends Specification {

  static NodeClassifier TEST_GEN = new NodeClassifier("test", "general")
  static NodeProperties PROPS = new NodeProperties("id", TEST_GEN)

  @AutoCleanup
  ExecHarness execControl = ExecHarness.harness()
  NodeRepository repo

  def setup() {
    repo = new InMemoryNodeRepository()
  }

  void "should be able to save and retrieve nodes"() {
    setup:
    def node = new Node(PROPS)

    when:
    execControl.executeSingle { repo.save(node) }

    and:
    def retrieved = execControl.yield { repo.get(PROPS) }.valueOrThrow

    then:
    node == retrieved
  }

  void "should be able to getOrCreate a node"() {
    when:
    def node = execControl.yield { repo.getOrCreate(PROPS) }.valueOrThrow

    then:
    node != null
    node.properties == PROPS
  }

  void "should be able to remove a node"() {
    when:
    def node = execControl.yield { repo.getOrCreate(PROPS) }.valueOrThrow

    then:
    node != null

    when:
    execControl.executeSingle { repo.remove(PROPS) }

    and:
    node = execControl.yield { repo.get(PROPS) }.valueOrThrow

    then:
    !node
  }

  void "should be able to list all nodes by a given classifier"() {
    setup:
    def node1 = new Node(new NodeProperties("id1", TEST_GEN))
    def node2 = new Node(new NodeProperties("id2", TEST_GEN))
    def node3 = new Node(new NodeProperties("id3", new NodeClassifier("foo", "bar")))

    when:
    [node1, node2, node3].each { n -> execControl.executeSingle { repo.save(n) } }

    and:
    def nodes = execControl.yield { repo.lookup(TEST_GEN) }.valueOrThrow

    then:
    2 == nodes.size()
    nodes*.id.containsAll(node1.properties.id, node2.properties.id)
    !nodes*.id.contains(node3.properties.id)
  }

  void "should be able to persistently relate nodes"() {
    setup:
    def node1 = new Node(new NodeProperties("id1", TEST_GEN))
    def node2 = new Node(new NodeProperties("id2", TEST_GEN))

    when:
    [node1, node2].each { n -> execControl.executeSingle { repo.save(n) } }

    and:
    execControl.executeSingle { repo.relate(node1, node2) }

    and:
    def upd1 = execControl.yieldSingle { repo.get(node1.properties) }.valueOrThrow
    def upd2 = execControl.yieldSingle { repo.get(node2.properties) }.valueOrThrow

    then:
    upd1.edge.hasRelationship(node2.properties)
    upd2.edge.hasDependent(node1.properties)
  }

  void "should be able to expire nodes by classifier and TTL"() {
    setup:
    def node1 = new Node(PROPS)
    def node2 = new Node(new NodeProperties("id2", TEST_GEN))

    when:
    [node1, node2].each { n -> execControl.executeSingle { repo.save(n) } }

    and:
    Thread.sleep(100)
    execControl.executeSingle { repo.get(node2.properties).operation() }
    execControl.executeSingle { repo.expireAll(TEST_GEN, 50) }

    and:
    def nodes = execControl.yieldSingle { repo.lookup(TEST_GEN) }.valueOrThrow

    then:
    1 == nodes.size()
    !nodes.contains(node1.properties)
    nodes[0] == node2.properties
  }
}
