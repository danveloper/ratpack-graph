package com.danveloper.ratpack.graph.internal;

import com.danveloper.ratpack.graph.*;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.Sets;
import ratpack.exec.Execution;
import ratpack.exec.Operation;
import ratpack.exec.Promise;
import ratpack.service.StartEvent;

import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class InMemoryNodeRepository implements NodeRepository {

  private Map<NodeProperties, Long> nodePropertiesIndex;
  private Map<NodeProperties, Set<NodeEdge.ModifyEvent>> nodeDependentsIndex;
  private Map<NodeProperties, Set<NodeEdge.ModifyEvent>> nodeRelationshipsIndex;
  private Map<NodeClassifier, Set<NodeProperties>> nodeClassifierIndex;

  private static class Caches {
    Cache<NodeProperties, Long> nodePropertiesIndexCache;
    Cache<NodeProperties, Set<NodeEdge.ModifyEvent>> nodeDependentsIndexCache;
    Cache<NodeProperties, Set<NodeEdge.ModifyEvent>> nodeRelationshipsIndexCache;
    Cache<NodeClassifier, Set<NodeProperties>> nodeClassifierIndexCache;
  }

  @Override
  public void onStart(StartEvent e) {
    ScheduledExecutorService executor = Execution.current().getController().getExecutor();
    Caches caches = new Caches();
    caches.nodePropertiesIndexCache = buildExpiringCache(executor).build();
    caches.nodeClassifierIndexCache = buildExpiringCache(executor).build();
    caches.nodeDependentsIndexCache = buildExpiringCache(executor)
        .<NodeProperties, Set<NodeEdge.ModifyEvent>>removalListener((props, deps, cause) -> {
          if (cause.wasEvicted()) {
            caches.nodePropertiesIndexCache.invalidate(props);
            caches.nodeRelationshipsIndexCache.invalidate(props);
          }
        }).build();
    caches.nodeRelationshipsIndexCache = buildExpiringCache(executor)
        .<NodeProperties, Set<NodeEdge.ModifyEvent>>removalListener((props, deps, cause) -> {
          if (cause.wasEvicted()) {
            caches.nodePropertiesIndexCache.invalidate(props);
            caches.nodeDependentsIndexCache.invalidate(props);
          }
        }).build();

    nodePropertiesIndex = caches.nodePropertiesIndexCache.asMap();
    nodeDependentsIndex = caches.nodeDependentsIndexCache.asMap();
    nodeRelationshipsIndex = caches.nodeRelationshipsIndexCache.asMap();
    nodeClassifierIndex = caches.nodeClassifierIndexCache.asMap();
  }

  @SuppressWarnings("unchecked")
  private static <K, V> Caffeine<K, V> buildExpiringCache(ScheduledExecutorService executor) {
    return (Caffeine<K, V>) Caffeine.newBuilder().executor(executor).expireAfterAccess(5, TimeUnit.MINUTES);
  }

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
      long trueLastAccessTime = node.getLastAccessTime();
      if (nodePropertiesIndex.containsKey(node.getProperties())) {
        long existingLastAccessTime = nodePropertiesIndex.get(node.getProperties());
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
