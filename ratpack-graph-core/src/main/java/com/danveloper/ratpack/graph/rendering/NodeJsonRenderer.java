package com.danveloper.ratpack.graph.rendering;

import com.danveloper.ratpack.graph.Node;
import com.danveloper.ratpack.graph.NodeConverter;
import com.google.common.collect.Iterables;
import ratpack.handling.Context;
import ratpack.render.Renderer;

import static com.danveloper.ratpack.graph.rendering.internal.NodeRenderingUtil.findConverter;
import static ratpack.jackson.Jackson.json;

public class NodeJsonRenderer implements Renderer<Node> {
  @Override
  public Class<Node> getType() {
    return Node.class;
  }

  @Override
  public void render(Context context, Node node) throws Exception {
    NodeConverter<?> converter = findConverter(context, node);
    converter.convert(node).then(o -> context.render(json(o)));
  }
}
