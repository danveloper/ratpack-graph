package com.danveloper.ratpack.graph;

import com.danveloper.ratpack.graph.internal.InMemoryNodeDataRepository;
import com.danveloper.ratpack.graph.internal.InMemoryNodeRepository;
import com.danveloper.ratpack.graph.rendering.NodeCollectionJsonRenderer;
import com.danveloper.ratpack.graph.rendering.NodeJsonRenderer;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import ratpack.render.Renderer;

public class GraphModule implements Module {
  @Override
  public void configure(Binder binder) {
    binder.bind(NodeRepository.class).to(InMemoryNodeRepository.class).in(Scopes.SINGLETON);
    binder.bind(NodeDataRepository.class).to(InMemoryNodeDataRepository.class).in(Scopes.SINGLETON);
    Multibinder.newSetBinder(binder, Renderer.class).addBinding().to(NodeJsonRenderer.class).in(Scopes.SINGLETON);
    Multibinder.newSetBinder(binder, Renderer.class).addBinding().to(NodeCollectionJsonRenderer.class).in(Scopes.SINGLETON);
  }
}
