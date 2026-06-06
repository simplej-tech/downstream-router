package com.example.downstream;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class StandardDownstreamApplication {
    public static void main(String[] args) {
        SpringApplication.run(StandardDownstreamApplication.class, args);
    }
}
