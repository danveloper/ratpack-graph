package com.danveloper.ratpack.graph;

import ratpack.exec.Promise;

/**
 * An interface for converting {@link Node} types to a representative object.
 * As an example, this class can pull the node's data object from the {@link NodeDataRepository} to build and provide a representative model object.
 *
 * @param <T> the representative object type
 */
public interface NodeConverter<T> {

  /**
   * The {@link NodeClassifier} for nodes that this converter is capable of converting.
   *
   * @return {@link NodeClassifier}
   */
  NodeClassifier getClassifier();

  /**
   * This method is provided with the {@link Node} type and returns a promise to the representative object.
   *
   * @param node the node to be converted
   * @return a promise to the node's representative object
   */
  Promise<T> convert(Node node);
}
