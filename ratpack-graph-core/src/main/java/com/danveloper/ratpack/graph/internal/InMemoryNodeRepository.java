package com.danveloper.ratpack.graph.internal;

import com.danveloper.ratpack.graph.*;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import ratpack.exec.Operation;
import ratpack.exec.Promise;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
      node = save0(node); // update lastAccessTime
    }
    return Promise.value(node);
  }

  @Override
  public Promise<Node> read(NodeProperties nodeProperties) {
    Node node = nodePropertiesIndex.containsKey(nodeProperties) ?
        nodePropertiesIndex.get(nodeProperties) : null;
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
        if (related != null) {
          related.getEdge().removeDependent(nodeProperties);
          save0(related);
        }
      });
      node.getEdge().dependents().forEach(props -> {
        Node dependent = nodePropertiesIndex.get(props);
        if (dependent != null) {
          dependent.getEdge().removeRelationship(nodeProperties);
          save0(dependent);
        }
      });
      if (nodeClassifierIndex.containsKey(nodeProperties.getClassifier()) &&
          nodeClassifierIndex.get(nodeProperties.getClassifier()).contains(nodeProperties)) {
        nodeClassifierIndex.get(nodeProperties.getClassifier()).remove(nodeProperties);
      }
      nodePropertiesIndex.remove(nodeProperties);
    }
  }

  private Node save0(Node node) {
    if (node != null && node.getProperties() != null && node.getProperties().getId() != null) {
      Node existing = nodePropertiesIndex.containsKey(node.getProperties()) ? nodePropertiesIndex.get(node.getProperties()) : null;
      NodeEdge newEdge;
      if (existing != null) {
        Set<NodeProperties> updatedDeps = existing.getEdge().dependents().stream().filter(props ->
            !node.getEdge().getDependentsMarkedForRemoval().contains(props)).collect(Collectors.toSet());
        Set<NodeProperties> updatedRels = existing.getEdge().relationships().stream().filter(props ->
            !node.getEdge().getRelationshipsMarkedForRemoval().contains(props)).collect(Collectors.toSet());

        Set<NodeProperties> depsToAdd = node.getEdge().dependents().stream().filter(props ->
            !existing.getEdge().hasDependent(props)).collect(Collectors.toSet());
        Set<NodeProperties> relsToAdd = node.getEdge().relationships().stream().filter(props ->
            !existing.getEdge().hasRelationship(props)).collect(Collectors.toSet());

        Set<NodeProperties> deps = Sets.newHashSet();
        deps.addAll(updatedDeps);
        deps.addAll(depsToAdd);

        Set<NodeProperties> rels = Sets.newHashSet();
        rels.addAll(updatedRels);
        rels.addAll(relsToAdd);

        newEdge = new NodeEdge(deps, rels);
      } else {
        newEdge = new NodeEdge(Sets.newHashSet(node.getEdge().dependents()), Sets.newHashSet(node.getEdge().relationships()));
      }

      Node updated = new Node(node.getProperties(), newEdge, System.currentTimeMillis());

      nodePropertiesIndex.put(node.getProperties(), updated);
      if (!nodeClassifierIndex.containsKey(node.getProperties().getClassifier())) {
        nodeClassifierIndex.put(node.getProperties().getClassifier(), Sets.newConcurrentHashSet());
      }
      if (!nodeClassifierIndex.get(node.getProperties().getClassifier()).contains(node.getProperties())) {
        nodeClassifierIndex.get(node.getProperties().getClassifier()).add(node.getProperties());
      }
      return updated;
    } else {
      throw new IllegalStateException("Somebody tried to insert an empty node");
    }
  }
}
