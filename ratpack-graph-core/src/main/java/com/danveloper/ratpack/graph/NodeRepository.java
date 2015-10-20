package com.danveloper.ratpack.graph;

import ratpack.exec.Operation;
import ratpack.exec.Promise;
import ratpack.server.Service;

import java.util.Set;

/**
 * A persistent repository for storing and accessing {@link Node}s.
 */
public interface NodeRepository extends Service {

  /**
   * Persists the provided node.
   *
   * @param node the node to be persisted
   */
  Operation save(Node node);

  /**
   * Lookup the properties for all nodes with the given {@link NodeClassifier}.
   *
   * @param classifier the {@link NodeClassifier} to be looked up
   * @return a promise to a set of node properties matching the provided {@link NodeClassifier}
   */
  Promise<Set<NodeProperties>> lookup(NodeClassifier classifier);

  /**
   * Gets the fully hydrated {@link Node} for the provided {@link NodeProperties}.
   *
   * @param properties the properties of the node
   * @return a promise to the node represented by the provided {@link NodeProperties}
   */
  Promise<Node> get(NodeProperties properties);

  /**
   * Performs a sort-of "update-or-insert" like functionality.
   * If a {@link Node} exists for the provided {@link NodeProperties}, then it is returned.
   * If a {@link Node} does not exist, then one is created and returned.
   *
   * @param properties the {@link NodeProperties} representing the node
   * @return a promise to a fully hydrated {@link Node}
   */
  Promise<Node> getOrCreate(NodeProperties properties);

  /**
   * Creates a persistent relationship from node "left" to node "right".
   * This means that calling {@link NodeEdge#hasRelationship(NodeProperties)} on "left" with the {@link NodeProperties} of "right" will return true.
   * Similarly, this creates a persistent dependency of "left" to "right".
   * This means that calling {@link NodeEdge#hasDependent(NodeProperties)} on "right" with the {@link NodeProperties} of "left" will return true.
   *
   * This method can be read as, "relate node, 'left', TO node, 'right'."
   *
   * @param left the dependent node
   * @param right the relateable node
   */
  Operation relate(Node left, Node right);

  /**
   * Removes the node from the persistent storage and removes any edge references to related or dependent nodes.
   *
   * @param properties the properties that represents the node
   */
  Operation remove(NodeProperties properties);

  /**
   * Will expire any nodes that are stored with the matching classifier and have not been accessed in time less than the specified TTL.
   *
   * @param classifier the classifier for the nodes to expire
   * @param ttl the Time-to-Live in milliseconds
   */
  Operation expireAll(NodeClassifier classifier, Long ttl);
}
