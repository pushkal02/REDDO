import amqp from 'amqplib';
import { randomUUID } from 'crypto';
import { config } from './config';
import { checkTaskStatus, updateTaskStatus, completeTaskAndProgress, handleTaskFailure } from './db';
import { executeTaskWithChaos } from './chaos';
import { logger } from './logger';

let connection: amqp.ChannelModel | null = null;
let channel: amqp.Channel | null = null;

export async function connectRabbitMQ() {
  try {
    logger.info(`Connecting to broker at ${config.rabbitmqUrl}...`);
    const conn = await amqp.connect(config.rabbitmqUrl);
    connection = conn;

    logger.info(`Channel establishing...`);
    const chan = await conn.createChannel();
    channel = chan;

    logger.info(`Channel established. Setting prefetch to 1.`);
    await chan.prefetch(1);

    // Assert queue (just in case, though Gateway asserts it too)
    await chan.assertQueue(config.nodeQueue, { durable: true });

    logger.info(`Listening for messages on queue: ${config.nodeQueue}`);
    
    await chan.consume(config.nodeQueue, async (msg) => {
      if (!msg) {
        logger.warn('Received empty consumer cancellation message.');
        return;
      }

      const contentStr = msg.content.toString();
      let payload: any;
      try {
        payload = JSON.parse(contentStr);
      } catch (err: any) {
        logger.error(`Failed to parse message body as JSON. Discarding. Content: ${contentStr}`);
        chan.ack(msg);
        return;
      }

      const taskExecutionId = payload.task_execution_id;
      const workflowInstanceId = payload.workflow_instance_id;
      const taskKey = payload.task_key;
      const inputData = payload.input_data || {};
      const correlationId = payload.correlation_id || workflowInstanceId || '-';
      const requestId = payload.request_id || '-';

      if (!taskExecutionId || !workflowInstanceId || !taskKey) {
        logger.error(`Missing required fields in task payload. Discarding. Payload: ${contentStr}`, correlationId, requestId);
        chan.ack(msg);
        return;
      }

      logger.info(`Received task message for execution ID: ${taskExecutionId}, key: ${taskKey}`, correlationId, requestId);

      try {
        // 1. Idempotency Check
        const currentStatus = await checkTaskStatus(taskExecutionId);
        if (!currentStatus) {
          logger.error(`Task execution record ${taskExecutionId} not found in DB. Discarding.`, correlationId, requestId);
          chan.ack(msg);
          return;
        }

        if (currentStatus === 'COMPLETED' || currentStatus === 'FAILED' || currentStatus === 'ROLLED_BACK') {
          logger.info(`Task ${taskExecutionId} has already run (Status: ${currentStatus}). Acknowledging message and exiting.`, correlationId, requestId);
          chan.ack(msg);
          return;
        }

        // 2. Set task status to RUNNING in database
        await updateTaskStatus(taskExecutionId, 'RUNNING', undefined, undefined, correlationId, requestId);

        // 3. Execute with Chaos simulation
        await executeTaskWithChaos(taskExecutionId, taskKey, inputData, correlationId, requestId);

        // 4. Update status and progress DAG on success
        await completeTaskAndProgress(taskExecutionId, correlationId, requestId, publishTaskMessage);

        // 5. Acknowledge message
        chan.ack(msg);
        logger.info(`Task ${taskExecutionId} completed & acknowledged successfully.`, correlationId, requestId);

      } catch (error: any) {
        logger.error(`Task ${taskExecutionId} execution failed: ${error.message}`, correlationId, requestId);
        
        try {
          // 6. Handle task failure in the DB (compensations and Saga transitions)
          await handleTaskFailure(workflowInstanceId, taskExecutionId, error.message, correlationId, requestId, publishTaskMessage);
          
          // 7. Poison Pill Prevention: reject without requeueing
          chan.nack(msg, false, false);
          logger.warn(`Rejected message for task ${taskExecutionId} without requeuing.`, correlationId, requestId);
        } catch (dbErr: any) {
          logger.error(`Failed to execute failure handler in database: ${dbErr.message}`, correlationId, requestId);
          // If database itself fails, we reject with requeue=true so that when database recovers, worker can try again.
          chan.nack(msg, false, true);
        }
      }
    });

  } catch (err: any) {
    logger.error(`Connection / subscription error: ${err.message}`);
    throw err;
  }
}

export async function publishTaskMessage(
  taskExecutionId: string,
  workflowInstanceId: string,
  taskKey: string,
  inputData: any,
  worker: string,
  correlationId: string,
  requestId: string
): Promise<void> {
  if (!channel) {
    throw new Error('RabbitMQ channel not initialized');
  }

  let routingKey: string;
  if (worker.toLowerCase() === 'java') {
    routingKey = 'tasks.java';
  } else if (worker.toLowerCase() === 'node') {
    routingKey = 'tasks.node';
  } else {
    throw new Error(`Unsupported worker type: ${worker}`);
  }

  // Generate a new request ID for tracing this outgoing task dispatch
  const nextRequestId = randomUUID();

  const payload = {
    task_execution_id: taskExecutionId,
    workflow_instance_id: workflowInstanceId,
    task_key: taskKey,
    input_data: inputData,
    correlation_id: correlationId,
    request_id: nextRequestId,
  };

  const body = Buffer.from(JSON.stringify(payload));
  logger.info(`Publishing next task ${taskKey} (${taskExecutionId}) to exchange ${config.workflowExchange} using routing key ${routingKey}`, correlationId, nextRequestId);

  channel.publish(config.workflowExchange, routingKey, body, {
    persistent: true,
    contentType: 'application/json',
  });
}

export async function closeRabbitMQ() {
  logger.info('Closing channel and connection...');
  try {
    if (channel) {
      await channel.close();
    }
    if (connection) {
      await connection.close();
    }
    logger.info('Connection closed.');
  } catch (err: any) {
    logger.error(`Error closing connection: ${err.message}`);
  }
}
