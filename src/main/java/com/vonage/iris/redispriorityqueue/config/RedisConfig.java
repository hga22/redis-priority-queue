package com.vonage.iris.redispriorityqueue.config;

import io.lettuce.core.RedisClient;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Setter
@Configuration
@ConfigurationProperties(prefix = "iris.redis")
public class RedisConfig {

  private String url;

  @Bean
  public RedisClient redisClient() {
    return RedisClient.create(url);
  }
}
