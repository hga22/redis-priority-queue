package com.vonage.iris.redispriorityqueue.controller;

import com.vonage.iris.redispriorityqueue.dto.Message;
import com.vonage.iris.redispriorityqueue.service.PriorityQueueService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/iris")
public class PriorityQueueController {

  @Autowired
  private PriorityQueueService priorityQueueService;

  @PostMapping(path = "{queueName}/add")
  public void add(@PathVariable("queueName") String queueName, @RequestBody Message message) {
    priorityQueueService.add(queueName, message);
  }

  @GetMapping(path = "{queueName}/get")
  public Message get(@PathVariable("queueName") String queueName) {
    return priorityQueueService.getMaxPriorityMessage(queueName);
  }
}
