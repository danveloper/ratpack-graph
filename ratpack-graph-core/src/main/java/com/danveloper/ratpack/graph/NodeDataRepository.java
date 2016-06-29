package com.danveloper.ratpack.graph;

import ratpack.exec.Operation;
import ratpack.exec.Promise;
import ratpack.service.Service;

/**
 * A repository for storing data objects that are associated with a {@link Node}
 */
public interface NodeDataRepository extends Service {

  /**
   * Retrieves the data object that is stored for the node represented by the provided properties.
   *
   * @param properties the properties representing the node
   * @param <T> the type of the data object
   * @return a promise to the data object that is stored for the node represented by the provided properties
   */
  <T> Promise<T> get(NodeProperties properties);

  /**
   * Saves the data object for the node represented by the provided properties.
   * Existing data will be overwritten with this call.
   *
   * @param properties the properties representing the node
   * @param object the data to store for the node
   */
  Operation save(NodeProperties properties, Object object);

  /**
   * Removes any data objects that are stored for the node represented by the provided properties
   *
   * @param properties the properties representing the node
   */
  Operation remove(NodeProperties properties);
}
