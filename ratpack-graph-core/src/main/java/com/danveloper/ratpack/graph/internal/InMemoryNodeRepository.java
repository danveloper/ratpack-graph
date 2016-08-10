package com.danveloper.ratpack.graph.internal;

import com.danveloper.ratpack.graph.*;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import ratpack.exec.Operation;
import ratpack.exec.Promise;

import java.util.*;
import java.util.stream.Collectors;

public class InMemoryNodeRepository implements NodeRepository {
  private final Map<NodeProperties, Long> nodePropertiesIndex = Maps.newConcurrentMap();
  private final Map<NodeProperties, Set<NodeEdge.ModifyEvent>> nodeDependentsIndex = Maps.newConcurrentMap();
  private final Map<NodeProperties, Set<NodeEdge.ModifyEvent>> nodeRelationshipsIndex = Maps.newConcurrentMap();
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
    Node node = get(nodeProperties, true);
    return Promise.value(node);
  }

  @Override
  public Promise<Node> read(NodeProperties nodeProperties) {
    Node node = get(nodeProperties, false);
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
      nodeClassifierIndex.get(classifier).stream()
          .map(props -> get(props, false))
          .filter(node -> node != null && System.currentTimeMillis() - node.getLastAccessTime() > ttl)
          .map(Node::getProperties)
          .forEach(this::remove0);
    }
    return Operation.noop();
  }

  private void remove0(NodeProperties nodeProperties) {
    if (nodePropertiesIndex.containsKey(nodeProperties)) {
      Node node = get(nodeProperties, false);
      if (node != null) {
        node.getEdge().relationships().forEach(props -> {
          Node related = get(props, false);
          if (related != null) {
            related.getEdge().removeDependent(nodeProperties);
            save0(related);
          }
        });
        node.getEdge().dependents().forEach(props -> {
          Node dependent = get(props, false);
          if (dependent != null) {
            dependent.getEdge().removeRelationship(nodeProperties);
            save0(dependent);
          }
        });
      }
      if (nodeClassifierIndex.containsKey(nodeProperties.getClassifier()) &&
          nodeClassifierIndex.get(nodeProperties.getClassifier()).contains(nodeProperties)) {
        nodeClassifierIndex.get(nodeProperties.getClassifier()).remove(nodeProperties);
      }
      nodePropertiesIndex.remove(nodeProperties);
    }
  }

  private Node save0(Node node) {
    if (node != null && node.getProperties() != null && node.getProperties().getId() != null) {
      Long trueLastAccessTime = node.getLastAccessTime();
      if (nodePropertiesIndex.containsKey(node.getProperties())) {
        Long existingLastAccessTime = nodePropertiesIndex.get(node.getProperties());
        if (existingLastAccessTime > trueLastAccessTime) {
          trueLastAccessTime = existingLastAccessTime;
        }
      }
      nodePropertiesIndex.put(node.getProperties(), trueLastAccessTime);

      Set<NodeEdge.ModifyEvent> dependents = nodeDependentsIndex.getOrDefault(node.getProperties(), Sets.newConcurrentHashSet());
      Set<NodeEdge.ModifyEvent> relateds = nodeRelationshipsIndex.getOrDefault(node.getProperties(), Sets.newConcurrentHashSet());

      processModificationEvents(node.getEdge().getDependentEvents(), dependents);
      processModificationEvents(node.getEdge().getRelationshipEvents(), relateds);

      nodeDependentsIndex.put(node.getProperties(), dependents);
      nodeRelationshipsIndex.put(node.getProperties(), relateds);

      if (!nodeClassifierIndex.containsKey(node.getProperties().getClassifier())) {
        nodeClassifierIndex.put(node.getProperties().getClassifier(), Sets.newConcurrentHashSet());
      }
      if (!nodeClassifierIndex.get(node.getProperties().getClassifier()).contains(node.getProperties())) {
        nodeClassifierIndex.get(node.getProperties().getClassifier()).add(node.getProperties());
      }
      return get(node.getProperties(), false);
    } else {
      throw new IllegalStateException("Somebody tried to insert an empty node");
    }
  }

  private void processModificationEvents(List<NodeEdge.ModifyEvent> events, Set<NodeEdge.ModifyEvent> relateds) {
    events.stream().forEach(event -> {
      if (event.getEventType() == NodeEdge.ModifyEvent.EventType.ADD) {
        relateds.add(event);
      } else {
        Optional<NodeEdge.ModifyEvent> eventOptional = relateds.stream().filter(e ->
            e.getNodeProperties() == event.getNodeProperties() && e.getModifyTime() < event.getModifyTime()
        ).findFirst();
        if (eventOptional.isPresent()) {
          relateds.remove(eventOptional.get());
        }
      }
    });
  }

  private Node get(NodeProperties nodeProperties, boolean updateAccessTime) {
    if (nodePropertiesIndex.containsKey(nodeProperties)) {
      Long lastAccessTime = nodePropertiesIndex.get(nodeProperties);
      Set<NodeEdge.ModifyEvent> dependents = nodeDependentsIndex.getOrDefault(nodeProperties, Sets.newConcurrentHashSet());
      Set<NodeEdge.ModifyEvent> relateds = nodeRelationshipsIndex.getOrDefault(nodeProperties, Sets.newConcurrentHashSet());

      Set<NodeProperties> dependentRefs = dependents.stream().map(NodeEdge.ModifyEvent::getNodeProperties).collect(Collectors.toSet());
      Set<NodeProperties> relatedRefs = relateds.stream().map(NodeEdge.ModifyEvent::getNodeProperties).collect(Collectors.toSet());

      NodeEdge nodeEdge = new NodeEdge(Sets.newConcurrentHashSet(relatedRefs), Sets.newConcurrentHashSet(dependentRefs));
      Node upd = new Node(nodeProperties, nodeEdge, updateAccessTime ? System.currentTimeMillis() : lastAccessTime);

      return updateAccessTime ? save0(upd) : upd;
    } else {
      return null;
    }
  }
}
