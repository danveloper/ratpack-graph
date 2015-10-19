package com.danveloper.ratpack.graph

import com.danveloper.ratpack.graph.internal.InMemoryNodeDataRepository
import ratpack.test.exec.ExecHarness
import spock.lang.AutoCleanup
import spock.lang.Specification

class NodeDataRepositorySpec extends Specification {

  static NodeProperties PROPS = new NodeProperties("id", new NodeClassifier("test", "general"))

  @AutoCleanup
  ExecHarness execControl = ExecHarness.harness()
  NodeDataRepository repo

  def setup() {
    repo = new InMemoryNodeDataRepository()
  }

  void "should be able to store and retrieve data objects by properties"() {
    setup:
    def data = [foo: "bar"]

    when:
    execControl.executeSingle { repo.save(PROPS, data) }

    and:
    def upd = execControl.yieldSingle { repo.<Map>get(PROPS) }.valueOrThrow

    then:
    data == upd
  }

  void "should be able to remove data objects"() {
    setup:
    def data = [foo: "bar"]

    when:
    execControl.executeSingle { repo.save(PROPS, data) }

    and:
    def upd = execControl.yieldSingle { repo.<Map>get(PROPS) }.valueOrThrow

    then:
    upd

    when:
    execControl.executeSingle { repo.remove(PROPS) }

    and:
    upd = execControl.yieldSingle { repo.get(PROPS) }.valueOrThrow

    then:
    !upd
  }
}
