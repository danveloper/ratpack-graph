package com.danveloper.ratpack.graph;

/**
 * Represents a node in a graph.
 * Serves as a container to the node's leaves, provides metadata that represents the node, and provides the timestamp (in milliseconds) for when the node was last accessed.
 */
public class Node {
  private final NodeProperties properties;
  private final NodeEdge edge;
  private final Long lastAccessTime;

  public Node(NodeProperties properties) {
    this(properties, new NodeEdge(), System.currentTimeMillis());
  }

  public Node(NodeProperties properties, NodeEdge edge, Long lastAccessTime) {
    this.properties = properties;
    this.edge = edge;
    this.lastAccessTime = lastAccessTime;
  }

  /**
   * The identifying metadata representing this node.
   *
   * @return {@link NodeProperties}
   */
  public NodeProperties getProperties() {
    return properties;
  }

  /**
   * Provides references to the edges of the node.
   *
   * @return {@link NodeEdge}
   */
  public NodeEdge getEdge() {
    return edge;
  }

  /**
   * Provides the timestamp (in milliseconds) for the last time this node was accessed.
   * Can be useful for asynchronously expiring a node based on some TTL.
   *
   * @return timestamp (in milliseconds)
   */
  public Long getLastAccessTime() {
    return lastAccessTime;
  }



  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Node node = (Node) o;

    if (properties != null ? !properties.equals(node.properties) : node.properties != null) return false;
    return (edge != null ? edge.equals(node.edge) : node.edge != null);
  }

  @Override
  public int hashCode() {
    int result = properties != null ? properties.hashCode() : 0;
    result = 31 * result + (edge != null ? edge.hashCode() : 0);
    result = 31 * result + (lastAccessTime != null ? lastAccessTime.hashCode() : 0);
    return result;
  }
}
