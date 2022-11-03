package com.vonage.iris.redispriorityqueue.service;

import com.vonage.iris.redispriorityqueue.dto.Message;

public interface PriorityQueueService {

  void add(String queueName, Message message);

  Message getMaxPriorityMessage(String queueName);

  void deleteMessage(String queue, String key);
}
