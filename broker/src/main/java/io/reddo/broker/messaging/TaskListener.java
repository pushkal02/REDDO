package io.reddo.broker.messaging;

import com.rabbitmq.client.Channel;
import io.reddo.broker.dto.TaskMessage;
import io.reddo.broker.service.SagaOrchestrator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
@RequiredArgsConstructor
@Slf4j
public class TaskListener {

    private final SagaOrchestrator orchestrator;

    /**
     * Listens to messages on the "java-tasks" queue.
     * Manually acknowledges (ACK) or rejects (NACK) messages after processing.
     * Manual ACK is critical to ensure that a message is only removed from the queue
     * AFTER the worker successfully processes it (or explicitly handles its failure).
     */
    @RabbitListener(queues = "java-tasks")
    public void onTaskReceived(TaskMessage message, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) {
        // Set MDC diagnostic variables
        MDC.put("correlationId", message.getCorrelationID() != null ? message.getCorrelationID() : "-");
        MDC.put("requestId", message.getRequestID() != null ? message.getRequestID() : "-");

        log.info("[Listener] Consumed task message for execution ID: {}, key: {}", 
                message.getTaskExecutionID(), message.getTaskKey());

        try {
            // Process the task using the virtual-thread-based orchestrator
            orchestrator.processTask(message);

            // Acknowledge message successfully
            channel.basicAck(deliveryTag, false);
            log.info("[Listener] Acknowledged message delivery tag: {}", deliveryTag);

        } catch (Exception e) {
            log.error("[Listener] Critical exception in listener worker thread for task {}: {}", 
                    message.getTaskExecutionID(), e.getMessage());

            try {
                // POISON PILL PREVENTION:
                channel.basicNack(deliveryTag, false, false);
                log.warn("[Listener] Rejected (NACKed) message delivery tag {} without requeuing.", deliveryTag);
            } catch (IOException ioEx) {
                log.error("[Listener] Failed to NACK message: {}", ioEx.getMessage());
            }
        } finally {
            MDC.clear();
        }
    }
}
