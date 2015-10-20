package com.danveloper.ratpack.graph.redis;

import com.danveloper.ratpack.graph.NodeClassifier;
import com.danveloper.ratpack.graph.NodeProperties;
import com.lambdaworks.redis.RedisAsyncConnection;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisURI;
import ratpack.server.Service;
import ratpack.server.StartEvent;

public class RedisSupport implements Service {
  private final RedisGraphModule.Config config;
  private RedisClient redisClient;
  protected RedisAsyncConnection<String, String> connection;

  RedisSupport(RedisGraphModule.Config config) {
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

  protected String getCompositeId(NodeProperties props) {
    return String.format("%s:%s:%s", props.getId(), props.getClassifier().getType(), props.getClassifier().getCategory());
  }

  protected NodeProperties destructureCompositeId(String compositeId) {
    String[] parts = compositeId.split(":");
    return new NodeProperties(parts[0], new NodeClassifier(parts[1], parts[2]));
  }
}
