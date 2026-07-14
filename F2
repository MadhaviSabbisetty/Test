package com.fincore.CommunicationService.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public DefaultKafkaConsumerFactory<String, String> stringConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Force reading from the beginning
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // ABSOLUTE MANDATE: Ignore properties files and custom libraries. Force String.
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * THE BULKHEAD: Dedicated Thread Pool for Tier-0 OTPs.
     * Prevents End-of-Day PDF emails from starving the OTP pipeline.
     */
    @Bean("priorityOtpContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> priorityOtpContainerFactory() {

        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(stringConsumerFactory());
        // Match this concurrency to the number of partitions on 'fincore.auth.otp.priority' (e.g., 10-20)
        factory.setConcurrency(10);
        // Strict Manual Acknowledgment. We only ACK when the SMS/Email is securely handed to the Vendor.
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        // We set the poll interval high enough to outlast the Resilience4j max timeout.
        factory.getContainerProperties().setPollTimeout(3000);

        return factory;
    }
}
