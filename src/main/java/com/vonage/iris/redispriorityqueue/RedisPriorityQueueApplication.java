package com.vonage.iris.redispriorityqueue;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class RedisPriorityQueueApplication {

	public static void main(String[] args) {
		SpringApplication.run(RedisPriorityQueueApplication.class, args);
	}

}
