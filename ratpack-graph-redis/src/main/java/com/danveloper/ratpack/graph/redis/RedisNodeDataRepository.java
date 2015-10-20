package com.danveloper.ratpack.graph.redis;

import com.danveloper.ratpack.graph.NodeDataRepository;
import com.danveloper.ratpack.graph.NodeProperties;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Inject;
import ratpack.exec.Execution;
import ratpack.exec.Operation;
import ratpack.exec.Promise;

import static ratpack.util.Exceptions.uncheck;

public class RedisNodeDataRepository extends RedisSupport implements NodeDataRepository {

  private final ObjectMapper mapper;

  @Inject
  RedisNodeDataRepository(RedisGraphModule.Config config, ObjectMapper mapper) {
    super(config);
    this.mapper = mapper.copy().enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL, JsonTypeInfo.As.PROPERTY);
  }

  @Override
  public <T> Promise<T> get(NodeProperties properties) {
    return hget("data:all", getCompositeId(properties)).map(json -> {
      JsonNode jsonNode = mapper.readTree(json);
      JsonNode classNode = jsonNode.get("@class");
      if (classNode.isNull()) {
        throw new RuntimeException("Could not extract @class property from stored json");
      } else {
        String className = classNode.asText();
        Class clazz = Class.forName(className);
        return (T)mapper.readValue(json, clazz);
      }
    });
  }

  @Override
  public Operation save(NodeProperties properties, Object object) {
    String json = uncheck(() -> mapper.writeValueAsString(object));
    return hset("data:all", getCompositeId(properties), json);
  }

  @Override
  public Operation remove(NodeProperties properties) {
    return hdel("data:all", getCompositeId(properties)).operation();
  }

  private Promise<String> hget(String key, String id) {
    return Promise.<String>of(d ->
            Futures.addCallback(connection.hget(key, id), new FutureCallback<String>() {
              @Override
              public void onSuccess(String result) {
                if (result != null) {
                  d.success(result);
                } else {
                  d.success(null);
                }
              }

              @Override
              public void onFailure(Throwable t) {
                d.error(new RuntimeException("Failed to hget data", t));
              }
            })
    );
  }

  private Operation hset(String key, String id, String val) {
    return Promise.<Boolean>of(d ->
            Futures.addCallback(connection.hset(key, id, val), new FutureCallback<Boolean>() {
              @Override
              public void onSuccess(Boolean result) {
                d.success(true);
              }

              @Override
              public void onFailure(Throwable t) {
                d.error(new RuntimeException("Failed to hset data", t));
              }
            }, Execution.current().getEventLoop())
    ).operation();
  }

  private Promise<Boolean> hdel(String key, String id) {
    return Promise.<Boolean>of(d ->
            Futures.addCallback(connection.hdel(key, id), new FutureCallback<Long>() {
              @Override
              public void onSuccess(Long result) {
                if (result > 0) {
                  d.success(true);
                } else {
                  d.error(new RuntimeException("Failed to hdel data"));
                }
              }

              @Override
              public void onFailure(Throwable t) {
                d.error(new RuntimeException("Failed to hdel data", t));
              }
            })
    );
  }
}
