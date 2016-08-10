package com.danveloper.ratpack.graph;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Provides an interface for accessing the leaves of a {@link Node}.
 * The provided references to the edges are {@link NodeProperties}, which can be used to discover categorically organized and typed references to dependent or related {@link Node}s.
 * In the graph, a {@link Node}'s edges are defined as either "relationships" or "dependents".
 * A node's relationships are a dependent leaf of another node, and are thus represented as a "higher" leaf to a node.
 * A node's dependents are relationships of another node, and are thus represented as a "lower" leaf to a node.
 *
 * [node1 relationships]
 * |
 * [node1]
 * |
 * [node1 dependents]
 * |
 * |_[node2 relationships]
 *   |
 *   [node2]
 *   |
 *   [node2 dependents]
 *
 * In this depiction, "node2" is said to be a "dependent" of "node1", while "node1" is said to be a "relationship" of "node2".
 */
public class NodeEdge {
  private final Set<NodeProperties> relationships;
  private final Set<NodeProperties> dependents;

  private final List<ModifyEvent> relationshipEvents = Lists.newArrayList();
  private final List<ModifyEvent> dependentEvents = Lists.newArrayList();

  public NodeEdge() {
    this(Sets.newHashSet(), Sets.newHashSet());
  }

  public NodeEdge(Set<NodeProperties> relationships, Set<NodeProperties> dependents) {
    this.relationships = relationships;
    this.dependents = dependents;
  }

  /**
   * @return an immutable set of the relationship leaves
   */
  @JsonProperty("relationships")
  public Set<NodeProperties> relationships() {
    return Collections.unmodifiableSet(this.relationships);
  }

  /**
   * @return an immutable set of the dependent leaves
   */
  @JsonProperty("dependents")
  public Set<NodeProperties> dependents() {
    return Collections.unmodifiableSet(this.dependents);
  }

  /**
   * Creates a relationship between the current node and the node represented by the provided properties.
   *
   * @param properties the properties of the related node
   */
  public void addRelationship(NodeProperties properties) {
    this.relationshipEvents.add(new ModifyEvent(ModifyEvent.EventType.ADD, properties));
    this.relationships.add(properties);
  }

  /**
   * Creates a dependency between the current node and the node represented by the provided properties.
   *
   * @param properties the properties of the dependent node
   */
  public void addDependent(NodeProperties properties) {
    this.dependentEvents.add(new ModifyEvent(ModifyEvent.EventType.ADD, properties));
    this.dependents.add(properties);
  }

  /**
   * Removes the dependency of the node represented by the provided properties and the current node.
   *
   * @param properties the properties of the dependent node
   */
  public void removeDependent(NodeProperties properties) {
    if (this.dependents.contains(properties)) {
      this.dependentEvents.add(new ModifyEvent(ModifyEvent.EventType.REMOVE, properties));
      this.dependents.remove(properties);
    }
  }

  /**
   * Removes the current node's relationship with the node represented by the provided properties.
   *
   * @param properties the properties of the related node
   */
  public void removeRelationship(NodeProperties properties) {
    if (this.relationships.contains(properties)) {
      this.relationshipEvents.add(new ModifyEvent(ModifyEvent.EventType.REMOVE, properties));
      this.relationships.remove(properties);
    }
  }

  /**
   * Informs as to whether the node represented by the provided properties is dependent upon the current node.
   *
   * @param properties the properties of the potentially dependent node
   * @return true/false depending on whether the node represented by the provided properties is a dependent of the current node.
   */
  public boolean hasDependent(NodeProperties properties) {
    return this.dependents.contains(properties);
  }

  /**
   * Informs as to whether the current node has a relationship with the node represented by the provided properties
   *
   * @param properties the properties of the potentially related node
   * @return true/false depending on whether the current node has a relationship with the node represented by the provided properties
   */
  public boolean hasRelationship(NodeProperties properties) {
    return this.relationships.contains(properties);
  }

  public List<ModifyEvent> getRelationshipEvents() {
    return relationshipEvents;
  }

  public List<ModifyEvent> getDependentEvents() {
    return dependentEvents;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    NodeEdge nodeEdge = (NodeEdge) o;

    if (relationships != null ? !relationships.equals(nodeEdge.relationships) : nodeEdge.relationships != null)
      return false;
    return !(dependents != null ? !dependents.equals(nodeEdge.dependents) : nodeEdge.dependents != null);

  }

  @Override
  public int hashCode() {
    int result = relationships != null ? relationships.hashCode() : 0;
    result = 31 * result + (dependents != null ? dependents.hashCode() : 0);
    return result;
  }

  public static class ModifyEvent {
    public enum EventType {
      ADD, REMOVE
    }

    private EventType eventType;
    private NodeProperties nodeProperties;
    private final Long modifyTime = System.currentTimeMillis();

    private ModifyEvent(EventType eventType, NodeProperties nodeProperties) {
      this.eventType = eventType;
      this.nodeProperties = nodeProperties;
    }

    public EventType getEventType() {
      return eventType;
    }

    public NodeProperties getNodeProperties() {
      return nodeProperties;
    }

    public Long getModifyTime() {
      return modifyTime;
    }
  }
}
