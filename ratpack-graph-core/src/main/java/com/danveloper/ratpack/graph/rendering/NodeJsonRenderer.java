package com.danveloper.ratpack.graph.rendering;

import com.danveloper.ratpack.graph.Node;
import com.danveloper.ratpack.graph.NodeConverter;
import ratpack.handling.Context;
import ratpack.render.Renderer;

import java.util.HashMap;
import java.util.Map;

import static com.danveloper.ratpack.graph.rendering.internal.NodeRenderingUtil.findConverter;
import static ratpack.jackson.Jackson.json;

public class NodeJsonRenderer implements Renderer<Node> {
  private static final Map<String, String> NOT_FOUND = new HashMap<String, String>() {{
    put("status", "not_found");
  }};

  @Override
  public Class<Node> getType() {
    return Node.class;
  }

  @Override
  public void render(Context context, Node node) throws Exception {
    NodeConverter<?> converter = findConverter(context, node);
    converter.convert(node).then(o -> {
      if (o != null) {
        context.render(json(o));
      } else {
        context.getResponse().status(404);
        context.render(json(NOT_FOUND));
      }
    });
  }
}
