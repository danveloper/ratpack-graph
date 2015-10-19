package com.danveloper.ratpack.graph.internal;

import com.danveloper.ratpack.graph.NodeDataRepository;
import com.danveloper.ratpack.graph.NodeProperties;
import com.google.common.collect.Maps;
import ratpack.exec.Operation;
import ratpack.exec.Promise;

import java.util.Map;

public class InMemoryNodeDataRepository implements NodeDataRepository {
  private final Map<NodeProperties, Object> storage = Maps.newConcurrentMap();

  @Override
  public <T> Promise<T> get(NodeProperties properties) {
    return Promise.value(storage.containsKey(properties) ? ((T) storage.get(properties)) : null);
  }

  @Override
  public Operation save(NodeProperties properties, Object object) {
    storage.put(properties, object);
    return Operation.noop();
  }

  @Override
  public Operation remove(NodeProperties properties) {
    if (storage.containsKey(properties)) {
      storage.remove(properties);
    }
    return Operation.noop();
  }
}
