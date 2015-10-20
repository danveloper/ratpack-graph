package com.danveloper.ratpack.graph.redis

import com.danveloper.ratpack.graph.Node
import com.danveloper.ratpack.graph.NodeClassifier
import com.danveloper.ratpack.graph.NodeProperties
import com.lambdaworks.redis.RedisClient
import com.lambdaworks.redis.RedisConnection
import ratpack.test.exec.ExecHarness
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification
import redis.embedded.RedisServer

import static com.danveloper.ratpack.graph.redis.PortFinder.nextFree

class RedisNodeRepositorySpec extends Specification {
  static int port = nextFree()

  static NodeClassifier TEST_GEN = new NodeClassifier("test", "general")

  @AutoCleanup("stop")
  @Shared
  RedisServer redisServer = new RedisServer(port)

  @Shared
  def repo = new RedisNodeRepository(new RedisGraphModule.Config(port: port))

  @AutoCleanup
  ExecHarness execControl = ExecHarness.harness()

  def setupSpec() {
    redisServer.start()
    repo.onStart(null)
  }

  def cleanup() {
    RedisConnection<String, String> conn = new RedisClient("localhost", port).connect()
    conn.flushall()
  }

  void "should be able to store and retrieve a node"() {
    setup:
    def props = new NodeProperties("id", TEST_GEN)
    def node = new Node(props)

    when:
    execControl.executeSingle { repo.save(node) }

    and:
    def upd = execControl.yieldSingle { repo.get(props) }.valueOrThrow

    then:
    node == upd
  }

  void "should be able to lookup nodes by classifier"() {
    setup:
    def props1 = new NodeProperties("id1", TEST_GEN)
    def props2 = new NodeProperties("id2", new NodeClassifier("foo", "bar"))
    def node1 = new Node(props1)
    def node2 = new Node(props2)

    when:
    [node1, node2].each { n -> execControl.executeSingle { repo.save(n) } }

    and:
    def lookedUp = execControl.yieldSingle { repo.lookup(TEST_GEN) }.valueOrThrow

    then:
    1 == lookedUp.size()
    lookedUp[0] == props1
  }

  void "should be able to create a node with getOrCreate"() {
    setup:
    def props = new NodeProperties("id", TEST_GEN)

    when:
    def node = execControl.yieldSingle { repo.getOrCreate(props) }.valueOrThrow

    then:
    node
    node.properties == props
  }

  void "should be able to relate one node to another"() {
    setup:
    def props1 = new NodeProperties("id1", TEST_GEN)
    def props2 = new NodeProperties("id2", TEST_GEN)
    def node1 = new Node(props1)
    def node2 = new Node(props2)

    when:
    [node1, node2].each { n -> execControl.yieldSingle { repo.save(n) } }

    and:
    execControl.executeSingle { repo.relate(node1, node2) }

    and:
    def upd1 = execControl.yieldSingle { repo.get(props1) }.valueOrThrow

    then:
    0 == upd1.edge.dependents().size()
    1 == upd1.edge.relationships().size()
    upd1.edge.relationships()[0] == props2

    when:
    def upd2 = execControl.yieldSingle { repo.get(props2) }.valueOrThrow

    then:
    1 == upd2.edge.dependents().size()
    0 == upd2.edge.relationships().size()
    upd2.edge.dependents()[0] == props1
  }

  void "should be able to remove a node"() {
    when:
    def node = execControl.yieldSingle { repo.getOrCreate(new NodeProperties("id1", TEST_GEN)) }.valueOrThrow

    and:
    execControl.executeSingle { repo.remove(node.properties) }

    and:
    def upd = execControl.yieldSingle { repo.get(node.properties) }.valueOrThrow

    then:
    !upd
  }

  void "removing a node should remove references in relationships and dependents"() {
    setup:
    def node1 = new Node(new NodeProperties("id1", TEST_GEN))
    def node2 = new Node(new NodeProperties("id2", TEST_GEN))
    def node3 = new Node(new NodeProperties("id3", TEST_GEN))

    when:
    [node1, node2, node3].each { n -> execControl.executeSingle { repo.save(n) } }

    and:
    execControl.execute {
      repo.relate(node1, node2)
    }
    and:
    execControl.execute {
      repo.relate(node2, node3)
    }

    and:
    def upd1 = execControl.yieldSingle { repo.get(node1.properties) }.valueOrThrow
    def upd2 = execControl.yieldSingle { repo.get(node2.properties) }.valueOrThrow
    def upd3 = execControl.yieldSingle { repo.get(node3.properties) }.valueOrThrow

    then:
    0 == upd1.edge.dependents().size()
    1 == upd1.edge.relationships().size()
    1 == upd2.edge.dependents().size()
    1 == upd2.edge.relationships().size()
    1 == upd3.edge.dependents().size()
    0 == upd3.edge.relationships().size()

    when:
    execControl.executeSingle { repo.remove(node2.properties) }

    and:
    upd1 = execControl.yieldSingle { repo.get(node1.properties) }.valueOrThrow
    upd2 = execControl.yieldSingle { repo.get(node2.properties) }.valueOrThrow
    upd3 = execControl.yieldSingle { repo.get(node3.properties) }.valueOrThrow

    then:
    0 == upd1.edge.dependents().size()
    0 == upd1.edge.relationships().size()
    !upd2
    0 == upd3.edge.dependents().size()
    0 == upd3.edge.relationships().size()

    when:
    def lookedUp = execControl.yieldSingle { repo.lookup(TEST_GEN) }.valueOrThrow

    then:
    2 == lookedUp.size()
    !lookedUp*.properties.contains(node1.properties)
  }

  void "should be able to expire nodes by classifier and TTL"() {
    setup:
    def node1 = new Node(new NodeProperties("id1", TEST_GEN))
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
