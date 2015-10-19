package com.danveloper.ratpack.graph.rendering.internal;

import com.danveloper.ratpack.graph.Node;
import com.danveloper.ratpack.graph.NodeConverter;
import com.google.common.collect.Iterables;
import ratpack.handling.Context;

public class NodeRenderingUtil {

  public static NodeConverter<?> findConverter(Context context, Node node) {
    return Iterables.find(context.getAll(NodeConverter.class), c ->
        node.getProperties().getClassifier().equals(c.getClassifier()));
  }
}
