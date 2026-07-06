package io.reddo.broker.config;

import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitConfig {

    /**
     * Configures the Jackson JSON Message Converter for RabbitMQ.
     * This overrides the default Java Serialization converter, allowing the worker
     * to safely receive and parse JSON objects published by other services (like Go or Node).
     */
    @Bean
    public MessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }
}
