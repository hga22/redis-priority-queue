package com.vonage.iris.redispriorityqueue.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Message {
  private String key;
  private String payload;
  private long priority;
}
