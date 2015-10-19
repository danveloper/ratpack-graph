package com.danveloper.ratpack.graph.rendering;

import com.danveloper.ratpack.graph.Node;
import com.danveloper.ratpack.graph.NodeConverter;
import ratpack.exec.Promise;
import ratpack.handling.Context;
import ratpack.render.Renderer;
import ratpack.stream.Streams;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.danveloper.ratpack.graph.rendering.internal.NodeRenderingUtil.findConverter;
import static ratpack.jackson.Jackson.json;

public class NodeCollectionJsonRenderer implements Renderer<NodeCollection> {
  @Override
  public Class<NodeCollection> getType() {
    return NodeCollection.class;
  }

  @Override
  public void render(Context context, NodeCollection collection) throws Exception {
    Set<Node> nodes = collection.getNodes();
    List<Promise> promises = nodes.stream().map(node -> {
      NodeConverter<?> converter = findConverter(context, node);
      return converter.convert(node);
    }).collect(Collectors.toList());
    Streams.flatYield(r -> {
      int reqNum = new Long(r.getRequestNum()).intValue();
      if (reqNum < promises.size()) {
        return promises.get(reqNum);
      } else {
        return Promise.value(null);
      }
    }).toList().then(o -> {
      context.render(json(o));
    });
  }
}
