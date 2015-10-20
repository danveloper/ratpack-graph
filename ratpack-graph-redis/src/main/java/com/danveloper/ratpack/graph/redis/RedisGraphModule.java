package com.danveloper.ratpack.graph.redis;

import ratpack.guice.ConfigurableModule;

public class RedisGraphModule extends ConfigurableModule<RedisGraphModule.Config> {

  @Override
  protected void configure() {

  }

  public static class Config {
    private String password;
    private String host;
    private Integer port;

    public Config() {
      host = "127.0.0.1";
    }

    /**
     * Convenience constructor most of the time not used if you are using Ratpack Config.
     *
     * @param password Redis Password
     * @param host     Redis host address
     * @param port     Redis port to use
     */
    public Config(String password, String host, Integer port) {
      this.password = password;
      this.host = host;
      this.port = port;
    }

    /**
     * Get the password for Redis.
     *
     * @return The password configured to use with Redis
     */
    public String getPassword() {
      return password;
    }

    /**
     * Set the password for Redis.
     *
     * @param password The password to use when connecting to Redis
     */
    public void setPassword(String password) {
      this.password = password;
    }

    /**
     * Get the address for Redis.
     *
     * @return String of the host address for Redis
     */
    public String getHost() {
      return host;
    }

    /**
     * Set the address for Redis.
     *
     * @param host The address for Redis
     */
    public void setHost(String host) {
      this.host = host;
    }

    /**
     * The Redis port.
     *
     * @return The port for Redis
     */
    public Integer getPort() {
      return port;
    }

    /**
     * Set the redis port.
     *
     * @param port Which port to use for Redis
     */
    public void setPort(Integer port) {
      this.port = port;
    }
  }
}
