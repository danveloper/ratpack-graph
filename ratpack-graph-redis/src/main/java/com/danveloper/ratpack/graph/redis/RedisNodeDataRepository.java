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
  public RedisNodeDataRepository(RedisGraphModule.Config config, ObjectMapper mapper) {
    super(config);
    this.mapper = mapper.copy().enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL, JsonTypeInfo.As.PROPERTY);
  }

  @Override
  public <T> Promise<T> get(NodeProperties properties) {
    return hget("data:all", getCompositeId(properties)).map(json -> {
      if (json != null) {
        JsonNode jsonNode = mapper.readTree(json);
        JsonNode classNode = jsonNode.get("@class");
        if (classNode.isNull()) {
          throw new RuntimeException("Could not extract @class property from stored json");
        } else {
          String className = classNode.asText();
          Class clazz = Class.forName(className);
          return (T) mapper.readValue(json, clazz);
        }
      } else {
        return null;
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
    return Promise.<String>async(d ->
        connection.hget(key, id).handleAsync((result, failure) -> {
          if (failure == null) {
            if (result != null) {
              d.success(result);
            } else {
              d.success(null);
            }
          } else {
            d.error(new RuntimeException("Failed to hget data", failure));
          }
          return null;
        }, Execution.current().getEventLoop())
    );
  }

  private Operation hset(String key, String id, String val) {
    return Promise.<Boolean>async(d ->
        connection.hset(key, id, val).handleAsync((result, failure) -> {
          if (failure == null) {
            d.success(result);
          } else {
            d.error(new RuntimeException("Failed to hset data", failure));
          }
          return null;
        }, Execution.current().getEventLoop())
    ).operation();
  }

  private Promise<Boolean> hdel(String key, String id) {
    return Promise.<Boolean>async(d ->
        connection.hdel(key, id).handleAsync( (result, failure) -> {
          if (failure == null) {
            if (result > 0) {
              d.success(true);
            }
          } else {
            d.error(new RuntimeException("Failed to hdel data", failure));
          }
          return null;
        }, Execution.current().getEventLoop())
    );
  }
}
