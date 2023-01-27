package com.by.UpdatesService.Kafka;

import com.by.UpdatesService.Model.Employee;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.lang.reflect.Field;

@Component
public class KafkaConsumer {

    @Autowired
    KafkaTemplate<String, Employee> kafkaTemplate;

    @Autowired
    KafkaTemplate<String, String> kafkaTemplateDql;

    @KafkaListener(topics = "app_update", groupId = "by_group")
    public void consume(@Payload Employee employee) throws IllegalAccessException {

        try {

            for (Field field : Employee.class.getDeclaredFields() ) {
                if (field.get(employee) == null) {
                    throw new Exception("Payload contains one or more null fields");
                }
            }

            kafkaTemplate.send("employee_updates", employee);
        } catch (Exception e) {
            kafkaTemplateDql.send("DLQ", e.getMessage());
        }
    }
}
