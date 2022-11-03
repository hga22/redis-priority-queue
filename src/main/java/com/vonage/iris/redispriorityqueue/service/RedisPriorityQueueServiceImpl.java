package com.vonage.iris.redispriorityqueue.service;

import com.vonage.iris.redispriorityqueue.dto.Message;
import io.lettuce.core.KeyValue;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
@Service
public class RedisPriorityQueueServiceImpl implements PriorityQueueService {

  private static final String QUEUE_NAME_PREFIX = "iris.connectors.priorityqueue";
  private static final String IRIS_QUEUE_LIST = "iris.connectors.priorityqueue.list";
  private static final String FIELD_PRIORITY = "p";
  private static final String FIELD_MESSAGE = "m";
  private static final int QUEUE_ACKNOWLEDGEMENT_TIMEOUT = 30;
  @Autowired
  private RedisClient redisClient;
  private StatefulRedisConnection<String, String> connection;

  @PostConstruct
  public void initializeConnection() {
    connection = redisClient.connect();
  }

  @Override
  public void add(String queueName, Message message) {
    RedisCommands<String, String> command = connection.sync();
    String fullQueueName = getFullQueueName(queueName);
    String hashKey = getHashKey(queueName, message.getKey());
    try {
      command.hmset(hashKey, Map.of(
          FIELD_PRIORITY, String.valueOf(message.getPriority()),
          FIELD_MESSAGE, message.getPayload()
      ));
      command.zadd(fullQueueName, message.getPriority(), message.getKey());
      command.sadd(IRIS_QUEUE_LIST, fullQueueName);
      log.info("operation={} queueName={} key={} message={} priority={}", "ADD", fullQueueName,
          message.getKey(), message.getPayload(), message.getPriority());
    } catch (RuntimeException e) {
      log.info("Error while adding to queue.", e);
      throw e;
    }
  }

  @Override
  public Message getMaxPriorityMessage(String queueName) {
    RedisCommands<String, String> command = connection.sync();
    String fullQueueName = getFullQueueName(queueName);
    String pendingQueueName = getPendingQueueName(queueName);

    try {
      ScoredValue<String> data = command.zpopmax(fullQueueName);
      if(data == null || data.isEmpty()) {
        return null;
      }
      long expiryTime = Instant.now().plus(QUEUE_ACKNOWLEDGEMENT_TIMEOUT, ChronoUnit.SECONDS).toEpochMilli();
      command.zadd(pendingQueueName, expiryTime, data.getValue());

      String hashKey = getHashKey(queueName, data.getValue());

      List<KeyValue<String, String>> d = command.hmget(hashKey, FIELD_MESSAGE);

      log.info("operation={} queueName={} message={} priority={}", "GET", fullQueueName, data.getValue(), data.getScore());
      return Message.builder().key(data.getValue()).payload(d.get(0).getValue()).priority((long)data.getScore()).build();
    } catch (RuntimeException e) {
      log.info("Error while popping from queue.", e);
      throw e;
    }
  }

  @Override
  public void deleteMessage(String queueName, String key) {
    RedisCommands<String, String> command = connection.sync();
    String pendingQueueName = getPendingQueueName(queueName);
    String hashKey = getHashKey(queueName, key);

    try {
      Long count = command.zrem(pendingQueueName, key);
      if(count == null || count == 0) {
        log.info("No message found with key={}", key);
        return;
      }
      command.hdel(hashKey, FIELD_MESSAGE, FIELD_PRIORITY);
      log.info("operation={} queueName={} key={}", "DEL", pendingQueueName, key);
    } catch (RuntimeException e) {
      log.info("Error while popping from queue.", e);
      throw e;
    }
  }

  @Scheduled(fixedDelay = 10000)
  private void processPendingAckMessage() {
    log.info("Running clean up job");
    RedisCommands<String, String> command = connection.sync();

    long currentTime = Instant.now().toEpochMilli();
    Set<String> queues = command.smembers(IRIS_QUEUE_LIST);
    for(String queue: queues) {
      String pendingQueueName = String.format("%s.pending", queue);
      Long count = command.zremrangebyscore(pendingQueueName, Range.create(0, currentTime));
      log.info("operation={} queueName={} count={}", "PEN_DEL", pendingQueueName, count);
    }

    log.info("Clean up job completed");
  }

  private String getHashKey(String queueName, String key) {
    return String.format("%s.%s.%s", QUEUE_NAME_PREFIX, queueName, key);
  }

  private String getPendingQueueName(String queueNamePrefix) {
    return String.format("%s.%s.pending", QUEUE_NAME_PREFIX, queueNamePrefix);
  }

  private String getFullQueueName(String queueNameSuffix) {
    return String.format("%s.%s", QUEUE_NAME_PREFIX, queueNameSuffix);
  }
}
