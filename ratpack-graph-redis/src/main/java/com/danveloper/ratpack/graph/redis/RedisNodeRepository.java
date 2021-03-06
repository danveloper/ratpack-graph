package com.danveloper.ratpack.graph.redis;

import com.danveloper.ratpack.graph.*;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import ratpack.exec.Execution;
import ratpack.exec.Operation;
import ratpack.exec.Promise;
import ratpack.stream.Streams;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class RedisNodeRepository extends RedisSupport implements NodeRepository {

  @Inject
  public RedisNodeRepository(RedisGraphModule.Config config) {
    super(config);
  }

  @Override
  public Operation save(Node node) {
    return save(node, true);
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

  @Override
  public Promise<Node> read(NodeProperties properties) {
    return get(properties, false);
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
    return save(left, false).flatMap(() -> save(right, false).promise()).operation();
  }

  @Override
  public Operation remove(NodeProperties properties) {
    return get(properties).flatMap(node -> {
      if (node != null) {
        List<Operation> updateDepOps = node.getEdge().dependents().stream()
            .map(dependent ->
                get(dependent).flatMap(depNode -> {
                  if (depNode != null) {
                    depNode.getEdge().removeRelationship(properties);
                    return save(depNode).promise();
                  } else {
                    return purgeNode(dependent).operation().promise();
                  }
                }).operation()
            )
            .collect(Collectors.toList());
        List<Operation> updateRelOps = node.getEdge().relationships().stream()
            .map(related ->
                get(related).flatMap(relNode -> {
                  if (relNode != null) {
                    relNode.getEdge().removeDependent(properties);
                    return save(relNode).promise();
                  } else {
                    return purgeNode(related).operation().promise();
                  }
                }).operation()
            )
            .collect(Collectors.toList());

        Promise<Void> updateDepsPromise = mapListOpsToPromise(updateDepOps);
        Promise<Void> updateRelsPromise = mapListOpsToPromise(updateRelOps);

        return updateDepsPromise
            .flatMap(v -> updateRelsPromise)
            .flatMap(v -> purgeNode(properties));
      } else {
        return purgeNode(properties);
      }
    }).operation();
  }

  private Promise<Boolean> purgeNode(NodeProperties properties) {
    String compositeId = getCompositeId(properties);
    return removeIndexClassifier(properties.getClassifier(), properties.getId()).flatMap(() ->
        hdel("node:all", compositeId)
    ).flatMap(v ->
        del(String.format("dependents:%s", compositeId))
    ).flatMap(v ->
        del(String.format("relationships:%s", compositeId))
    );
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

  private Operation save(Node node, boolean cleanupLeaves) {
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
    ).flatMap(o -> {
      if (cleanupLeaves) {
        return remDependentsOpsPromise.flatMap(this::mapListOpsToPromise);
      } else {
        return Promise.value(null);
      }
    }).flatMap(o -> {
      if (cleanupLeaves) {
        return remRelationshipOpsPromise.flatMap(this::mapListOpsToPromise);
      } else {
        return Promise.value(null);
      }
    }).operation();
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
    return Streams.publish(ops).flatMap(Operation::promise).toList().operation().promise();
  }

  private Operation indexClassifier(NodeClassifier classifier, String id) {
    return sadd(getClassifierId(classifier), id);
  }

  private Operation removeIndexClassifier(NodeClassifier classifier, String id) {
    return srem(getClassifierId(classifier), String.format("%s:%s:%s", id, classifier.getType(), classifier.getCategory()));
  }

  private String getClassifierId(NodeClassifier classifier) {
    return String.format("classifier:%s:%s", classifier.getType(), classifier.getCategory());
  }

  private Promise<Set<String>> smembers(String key) {
    return Promise.<Set<String>>async(d ->
        connection.smembers(key).handleAsync((result, failure) -> {
          if (failure == null) {
            d.success(result != null ? result : Sets.<String>newHashSet());
          } else {
            d.error(new RuntimeException("Failed to smembers", failure));
          }
          return null;
        }, Execution.current().getEventLoop())
    );
  }

  private Operation sadd(String key, String id) {
    return Promise.<Boolean>async(d ->
        connection.sadd(key, id).handleAsync((result, failure) -> {
          if (failure == null) {
            d.success(true);
          } else {
            d.error(new RuntimeException("Failed to sadd data", failure));
          }
          return null;
        }, Execution.current().getEventLoop())
    ).operation();
  }

  private Operation srem(String key, String id) {
    return Promise.<Boolean>async(d ->
        connection.srem(key, id).handleAsync((result, failure) -> {
          if (failure == null) {
            d.success(true);
          } else {
            d.error(new RuntimeException("Failed to srem data", failure));
          }
          return null;
        }, Execution.current().getEventLoop())
    ).operation();
  }

  private Operation hset(String key, String id, String val) {
    return Promise.<Boolean>async(d ->
        connection.hset(key, id, val).handleAsync((result, failure) -> {
          if (failure == null) {
            d.success(true);
          } else {
            d.error(new RuntimeException("Failed to hset data", failure));
          }
          return null;
        }, Execution.current().getEventLoop())
    ).operation();
  }

  private Promise<Long> hget(String key, String id) {
    return Promise.<Long>async(d ->
        connection.hget(key, id).handleAsync((result, failure) -> {
          if (failure == null) {
            if (result != null) {
              d.success(Long.valueOf(result));
            } else {
              d.success(null);
            }
          } else {
            d.error(new RuntimeException("Failed to hget data", failure));
          }
          return null;
        }, Execution.current().getEventLoop())
    );
  }

  private Promise<Boolean> hdel(String key, String id) {
    return Promise.<Boolean>async(d ->
        connection.hdel(key, id).handleAsync((result, failure) -> {
          if (failure == null) {
            d.success(true);
          }
          return null;
        }, Execution.current().getEventLoop())
    );
  }

  private Promise<Boolean> del(String key) {
    return Promise.<Boolean>async(d ->
        connection.del(key).handleAsync((result, failure) -> {
          if (failure == null) {
            d.success(true);
          }
          return null;
        }, Execution.current().getEventLoop())
    );
  }
}
