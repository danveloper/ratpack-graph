package com.danveloper.ratpack.graph.internal;

import com.danveloper.ratpack.graph.Node;
import com.danveloper.ratpack.graph.NodeClassifier;
import com.danveloper.ratpack.graph.NodeProperties;
import com.danveloper.ratpack.graph.NodeRepository;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import ratpack.exec.Operation;
import ratpack.exec.Promise;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class InMemoryNodeRepository implements NodeRepository {
  private final Map<NodeProperties, Node> nodePropertiesIndex = Maps.newConcurrentMap();
  private final Map<NodeClassifier, Set<NodeProperties>> nodeClassifierIndex = Maps.newConcurrentMap();

  @Override
  public Operation save(Node node) {
    return Operation.of(() -> save0(node));
  }

  @Override
  public Promise<Set<NodeProperties>> lookup(NodeClassifier classifier) {
    return Promise.value(nodeClassifierIndex.containsKey(classifier) ?
        Collections.unmodifiableSet(nodeClassifierIndex.get(classifier)) : null);
  }

  @Override
  public Promise<Node> get(NodeProperties nodeProperties) {
    Node node = nodePropertiesIndex.containsKey(nodeProperties) ?
        nodePropertiesIndex.get(nodeProperties) : null;
    if (node != null) {
      save0(node); // update lastAccessTime
    }
    return Promise.value(node);
  }

  @Override
  public Promise<Node> getOrCreate(NodeProperties nodeProperties) {
    if (!nodePropertiesIndex.containsKey(nodeProperties)) {
      Node node = new Node(nodeProperties);
      save0(node);
    }
    return get(nodeProperties);
  }

  @Override
  public Operation relate(Node left, Node right) {
    left.getEdge().addRelationship(right.getProperties());
    right.getEdge().addDependent(left.getProperties());
    return Operation.of(() -> {
      save0(left);
      save0(right);
    });
  }

  @Override
  public Operation remove(NodeProperties nodeProperties) {
    remove0(nodeProperties);
    return Operation.noop();
  }

  @Override
  public Operation expireAll(NodeClassifier classifier, Long ttl) {
    if (nodeClassifierIndex.containsKey(classifier)) {
      nodeClassifierIndex.get(classifier).stream().filter(props -> {
        Node node = nodePropertiesIndex.get(props);
        return System.currentTimeMillis() - node.getLastAccessTime() > ttl;
      }).forEach(this::remove0);
    }
    return Operation.noop();
  }

  private void remove0(NodeProperties nodeProperties) {
    if (nodePropertiesIndex.containsKey(nodeProperties)) {
      Node node = nodePropertiesIndex.get(nodeProperties);
      node.getEdge().relationships().forEach(props -> {
        Node related = nodePropertiesIndex.get(props);
        related.getEdge().removeDependent(nodeProperties);
        save0(related);
      });
      node.getEdge().dependents().forEach(props -> {
        Node dependent = nodePropertiesIndex.get(props);
        dependent.getEdge().removeRelationship(nodeProperties);
        save0(dependent);
      });
      if (nodeClassifierIndex.containsKey(nodeProperties.getClassifier()) &&
          nodeClassifierIndex.get(nodeProperties.getClassifier()).contains(nodeProperties)) {
        nodeClassifierIndex.get(nodeProperties.getClassifier()).remove(nodeProperties);
      }
      nodePropertiesIndex.remove(nodeProperties);
    }
  }

  private void save0(Node node) {
    if (node != null && node.getProperties() != null && node.getProperties().getId() != null) {
      long accessTime = System.currentTimeMillis();
      Node updated = new Node(node.getProperties(), node.getEdge(), accessTime);
      nodePropertiesIndex.put(node.getProperties(), updated);
      if (!nodeClassifierIndex.containsKey(node.getProperties().getClassifier())) {
        nodeClassifierIndex.put(node.getProperties().getClassifier(), Sets.newConcurrentHashSet());
      }
      if (!nodeClassifierIndex.get(node.getProperties().getClassifier()).contains(node.getProperties())) {
        nodeClassifierIndex.get(node.getProperties().getClassifier()).add(node.getProperties());
      }
    } else {
      throw new IllegalStateException("Somebody tried to insert an empty node");
    }
  }
}
