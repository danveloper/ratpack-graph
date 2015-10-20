package com.danveloper.ratpack.graph.redis;

import com.danveloper.ratpack.graph.*;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Inject;
import com.lambdaworks.redis.RedisAsyncConnection;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisURI;
import ratpack.exec.Execution;
import ratpack.exec.Operation;
import ratpack.exec.Promise;
import ratpack.server.StartEvent;
import ratpack.stream.Streams;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class RedisNodeRepository implements NodeRepository {

  private final RedisGraphModule.Config config;
  private RedisClient redisClient;
  private RedisAsyncConnection<String, String> connection;

  @Inject
  RedisNodeRepository(RedisGraphModule.Config config) {
    this.config = config;
  }

  @Override
  public void onStart(StartEvent e) {
    this.redisClient = new RedisClient(getRedisURI());
    this.connection = redisClient.connectAsync();
  }

  private RedisURI getRedisURI() {
    RedisURI.Builder builder = RedisURI.Builder.redis(config.getHost());

    if (config.getPassword() != null) {
      builder.withPassword(config.getPassword());
    }

    if (config.getPort() != null) {
      builder.withPort(config.getPort());
    }

    return builder.build();
  }

  @Override
  public Operation save(Node node) {
    String compositeId = getCompositeId(node.getProperties());
    Operation storeOp = hset("node:all", compositeId, Long.toString(System.currentTimeMillis()));
    Operation indexOp = indexClassifier(node.getProperties().getClassifier(), compositeId);
    Promise<Set<String>> dependentMembersPromise = smembers(String.format("dependents:%s", compositeId));
    Promise<Set<String>> relatedMembersPromise = smembers(String.format("relationships:%s", compositeId));
    Promise<List<Operation>> addDependentsOpsPromise = dependentMembersPromise.flatMap(dependents ->
            saddLeaf(compositeId, node.getEdge().dependents(), dependents, "dependents")
    );
    Promise<List<Operation>> addRelationshipOpsPromise = relatedMembersPromise.flatMap(relateds ->
            saddLeaf(compositeId, node.getEdge().relationships(), relateds, "relationships")
    );
    Promise<List<Operation>> remDependentsOpsPromise = dependentMembersPromise.flatMap(dependents ->
            sremLeaf(compositeId, node.getEdge().dependents(), dependents, "dependents")
    );
    Promise<List<Operation>> remRelationshipOpsPromise = relatedMembersPromise.flatMap(relateds ->
            sremLeaf(compositeId, node.getEdge().relationships(), relateds, "relationships")
    );

    return storeOp.flatMap(indexOp.promise()).flatMap(o ->
            addDependentsOpsPromise.flatMap(this::mapListOpsToPromise)
    ).flatMap(o ->
            addRelationshipOpsPromise.flatMap(this::mapListOpsToPromise)
    ).flatMap(o ->
            remDependentsOpsPromise.flatMap(this::mapListOpsToPromise)
    ).flatMap(o ->
            remRelationshipOpsPromise.flatMap(this::mapListOpsToPromise)
    ).operation();
  }

  @Override
  public Promise<Set<NodeProperties>> lookup(NodeClassifier classifier) {
    return smembers(String.format("classifier:%s:%s", classifier.getType(), classifier.getCategory())).flatMap(compositeIds ->
            Promise.value(compositeIds.stream().map(this::destructureCompositeId).collect(Collectors.toSet()))
    );
  }

  @Override
  public Promise<Node> get(NodeProperties properties) {
    return get(properties, true);
  }

  private Promise<Node> get(NodeProperties properties, boolean updateAccessTime) {
    String compositeId = getCompositeId(properties);
    return hget("node:all", compositeId).flatMap(lastAccessTime -> {
      if (lastAccessTime != null) {
        Promise<Set<String>> dependentMembersPromise = smembers(String.format("dependents:%s", compositeId));
        Promise<Set<String>> relatedMembersPromise = smembers(String.format("relationships:%s", compositeId));

        return dependentMembersPromise.flatMap(compositeIds ->
                Promise.value(compositeIds.stream().map(this::destructureCompositeId).collect(Collectors.toSet()))
        ).flatMap(dependentNodeProperties ->
                relatedMembersPromise.flatMap(compositeIds ->
                        Promise.value(compositeIds.stream().map(this::destructureCompositeId).collect(Collectors.toSet()))
                ).flatMap(relatedNodeProperties -> {
                  NodeEdge edge = new NodeEdge(relatedNodeProperties, dependentNodeProperties);
                  Node upd = new Node(properties, edge, updateAccessTime ? System.currentTimeMillis() : lastAccessTime);
                  return save(upd).map(() -> upd);
                })
        );
      } else {
        return Promise.value(null);
      }
    });
  }

  @Override
  public Promise<Node> getOrCreate(NodeProperties properties) {
    return get(properties).flatMap(n -> {
      if (n != null) {
        return Promise.value(n);
      } else {
        Node node = new Node(properties, new NodeEdge(), System.currentTimeMillis());
        return save(node).map(() -> node);
      }
    });
  }

  @Override
  public Operation relate(Node left, Node right) {
    left.getEdge().addRelationship(right.getProperties());
    right.getEdge().addDependent(left.getProperties());
    return save(left).flatMap(() -> save(right).promise()).operation();
  }

  @Override
  public Operation remove(NodeProperties properties) {
    return get(properties).flatMap(node -> {
      List<Operation> updateDepOps = node.getEdge().dependents().stream()
          .map(dependent ->
                  get(dependent).flatMap(depNode -> {
                    depNode.getEdge().removeRelationship(properties);
                    return save(depNode).promise();
                  }).operation()
          )
          .collect(Collectors.toList());
      List<Operation> updateRelOps = node.getEdge().relationships().stream()
          .map(related ->
                  get(related).flatMap(relNode -> {
                    relNode.getEdge().removeDependent(properties);
                    return save(relNode).promise();
                  }).operation()
          )
          .collect(Collectors.toList());

      Promise<Void> updateDepsPromise = mapListOpsToPromise(updateDepOps);
      Promise<Void> updateRelsPromise = mapListOpsToPromise(updateRelOps);

      String compositeId = getCompositeId(properties);

      return updateDepsPromise.flatMap(v -> updateRelsPromise)
          .flatMap(v -> removeIndexClassifier(properties.getClassifier(), compositeId).promise())
          .flatMap(v -> hdel("node:all", compositeId));
    }).operation();
  }

  @Override
  public Operation expireAll(NodeClassifier classifier, Long ttl) {
    return lookup(classifier).flatMap(props -> {
      List<NodeProperties> propsList = Lists.newArrayList(props);
      return Streams.flatYield(r -> {
        int reqNum = new Long(r.getRequestNum()).intValue();
        if (reqNum < propsList.size()) {
          return get(propsList.get(reqNum), false);
        } else {
          return Promise.value(null);
        }
      }).toList();
    }).map(nodes ->
      nodes.stream().filter(n -> System.currentTimeMillis() - n.getLastAccessTime() > ttl).collect(Collectors.toList())
    ).flatMap(expiredNodes ->
            Streams.flatYield(r -> {
              int reqNum = new Long(r.getRequestNum()).intValue();
              if (reqNum < expiredNodes.size()) {
                return remove(expiredNodes.get(reqNum).getProperties()).promise();
              } else {
                return Promise.value(null);
              }
            }).toList()
    ).operation();
  }

  private Promise<List<Operation>> saddLeaf(String compositeId, Set<NodeProperties> edges, Set<String> relateds, String prefix) {
    return Promise.value(edges.stream()
        .filter(props -> !relateds.contains(getCompositeId(props)))
        .map(props -> sadd(String.format("%s:%s", prefix, compositeId), getCompositeId(props)))
        .collect(Collectors.toList()));
  }

  private Promise<List<Operation>> sremLeaf(String compositeId, Set<NodeProperties> edges, Set<String> relateds, String prefix) {
    Set<String> edgeIds = edges.stream().map(this::getCompositeId).collect(Collectors.toSet());
    return Promise.value(relateds.stream()
        .filter(relatedCompositeId -> !edgeIds.contains(relatedCompositeId))
        .map(relatedCompositeId -> srem(String.format("%s:%s", prefix, compositeId), relatedCompositeId))
        .collect(Collectors.toList()));
  }

  private Promise<Void> mapListOpsToPromise(List<Operation> ops) {
    Promise<Void> p = null;
    for (Operation op : ops) {
      Promise<Void> opP = op.promise();
      if (p == null) {
        p = opP;
      } else {
        p = p.flatMap(v -> opP);
      }
    }
    return p == null ? Promise.value(null) : p;
  }

  private String getCompositeId(NodeProperties props) {
    return String.format("%s:%s:%s", props.getId(), props.getClassifier().getType(), props.getClassifier().getCategory());
  }

  private NodeProperties destructureCompositeId(String compositeId) {
    String[] parts = compositeId.split(":");
    return new NodeProperties(parts[0], new NodeClassifier(parts[1], parts[2]));
  }

  private Operation indexClassifier(NodeClassifier classifier, String id) {
    return sadd(getClassifierId(classifier), id);
  }

  private Operation removeIndexClassifier(NodeClassifier classifier, String id) {
    return srem(getClassifierId(classifier), id);
  }

  private String getClassifierId(NodeClassifier classifier) {
    return String.format("classifier:%s:%s", classifier.getType(), classifier.getCategory());
  }

  private Promise<Set<String>> smembers(String key) {
    return Promise.<Set<String>>of(d ->
            Futures.addCallback(connection.smembers(key), new FutureCallback<Set<String>>() {
              @Override
              public void onSuccess(Set<String> result) {
                d.success(result != null ? result : Sets.<String>newHashSet());
              }

              @Override
              public void onFailure(Throwable t) {
                d.error(new RuntimeException("Failed to smembers", t));
              }
            }, Execution.current().getEventLoop())
    );
  }

  private Operation sadd(String key, String id) {
    return Promise.<Boolean>of(d ->
            Futures.addCallback(connection.sadd(key, id), new FutureCallback<Long>() {
              @Override
              public void onSuccess(Long result) {
                d.success(true);
              }

              @Override
              public void onFailure(Throwable t) {
                d.error(new RuntimeException("Failed to sadd data", t));
              }
            })
    ).operation();
  }

  private Operation srem(String key, String id) {
    return Promise.<Boolean>of(d ->
            Futures.addCallback(connection.srem(key, id), new FutureCallback<Long>() {
              @Override
              public void onSuccess(Long result) {
                if (result > 0) {
                  d.success(true);
                } else {
                  d.error(new RuntimeException("Failed to srem data"));
                }
              }

              @Override
              public void onFailure(Throwable t) {
                d.error(new RuntimeException("Failed to srem data", t));
              }
            })
    ).operation();
  }

  private Operation hset(String key, String id, String val) {
    return Promise.<Boolean>of(d ->
            Futures.addCallback(connection.hset(key, id, val), new FutureCallback<Boolean>() {
              @Override
              public void onSuccess(Boolean result) {
                d.success(true);
              }

              @Override
              public void onFailure(Throwable t) {
                d.error(new RuntimeException("Failed to hset data", t));
              }
            }, Execution.current().getEventLoop())
    ).operation();
  }

  private Promise<Long> hget(String key, String id) {
    return Promise.<Long>of(d ->
            Futures.addCallback(connection.hget(key, id), new FutureCallback<String>() {
              @Override
              public void onSuccess(String result) {
                if (result != null) {
                  d.success(Long.valueOf(result));
                } else {
                  d.success(null);
                }
              }

              @Override
              public void onFailure(Throwable t) {
                d.error(new RuntimeException("Failed to hget data", t));
              }
            })
    );
  }

  private Promise<Boolean> hdel(String key, String id) {
    return Promise.<Boolean>of(d ->
            Futures.addCallback(connection.hdel(key, id), new FutureCallback<Long>() {
              @Override
              public void onSuccess(Long result) {
                if (result > 0) {
                  d.success(true);
                } else {
                  d.error(new RuntimeException("Failed to hdel data"));
                }
              }

              @Override
              public void onFailure(Throwable t) {
                d.error(new RuntimeException("Failed to hdel data", t));
              }
            })
    );
  }
}
