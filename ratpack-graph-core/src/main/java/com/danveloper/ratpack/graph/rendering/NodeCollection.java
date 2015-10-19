package com.danveloper.ratpack.graph.rendering;

import com.danveloper.ratpack.graph.Node;

import java.util.Set;

public class NodeCollection {
  private final Set<Node> nodes;

  public NodeCollection(Set<Node> nodes) {
    this.nodes = nodes;
  }

  public Set<Node> getNodes() {
    return this.nodes;
  }
}
