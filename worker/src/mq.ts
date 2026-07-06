import amqp from 'amqplib';
import { config } from './config';
import { checkTaskStatus, updateTaskStatus, completeTaskAndProgress, handleTaskFailure } from './db';
import { executeTaskWithChaos } from './chaos';

let connection: amqp.ChannelModel | null = null;
let channel: amqp.Channel | null = null;

export async function connectRabbitMQ() {
  try {
    console.log(`[RabbitMQ] Connecting to broker at ${config.rabbitmqUrl}...`);
    const conn = await amqp.connect(config.rabbitmqUrl);
    connection = conn;

    console.log(`[RabbitMQ] Channel establishing...`);
    const chan = await conn.createChannel();
    channel = chan;

    console.log(`[RabbitMQ] Channel established. Setting prefetch to 1.`);
    await chan.prefetch(1);

    // Assert queue (just in case, though Gateway asserts it too)
    await chan.assertQueue(config.nodeQueue, { durable: true });

    console.log(`[RabbitMQ] Listening for messages on queue: ${config.nodeQueue}`);
    
    await chan.consume(config.nodeQueue, async (msg) => {
      if (!msg) {
        console.warn('[RabbitMQ] Received empty consumer cancellation message.');
        return;
      }

      const contentStr = msg.content.toString();
      console.log(`[RabbitMQ] Received task payload: ${contentStr}`);

      let payload: any;
      try {
        payload = JSON.parse(contentStr);
      } catch (err) {
        console.error('[RabbitMQ] Failed to parse message body as JSON. Discarding.', err);
        chan.ack(msg);
        return;
      }

      const taskExecutionId = payload.task_execution_id;
      const workflowInstanceId = payload.workflow_instance_id;
      const taskKey = payload.task_key;
      const inputData = payload.input_data || {};

      if (!taskExecutionId || !workflowInstanceId || !taskKey) {
        console.error('[RabbitMQ] Missing required fields in task payload. Discarding.', payload);
        chan.ack(msg);
        return;
      }

      try {
        // 1. Idempotency Check
        const currentStatus = await checkTaskStatus(taskExecutionId);
        if (!currentStatus) {
          console.error(`[RabbitMQ] Task execution record ${taskExecutionId} not found in DB. Discarding.`);
          chan.ack(msg);
          return;
        }

        if (currentStatus === 'COMPLETED' || currentStatus === 'FAILED' || currentStatus === 'ROLLED_BACK') {
          console.log(`[RabbitMQ] Task ${taskExecutionId} has already run (Status: ${currentStatus}). Acknowledging message and exiting.`);
          chan.ack(msg);
          return;
        }

        // 2. Set task status to RUNNING in database
        await updateTaskStatus(taskExecutionId, 'RUNNING');

        // 3. Execute with Chaos simulation
        await executeTaskWithChaos(taskExecutionId, taskKey, inputData);

        // 4. Update status and progress DAG on success
        await completeTaskAndProgress(taskExecutionId, publishTaskMessage);

        // 5. Acknowledge message
        chan.ack(msg);
        console.log(`[RabbitMQ] Task ${taskExecutionId} completed & acknowledged successfully.`);

      } catch (error: any) {
        console.error(`[RabbitMQ] Task ${taskExecutionId} execution failed: ${error.message}`);
        
        try {
          // 6. Handle task failure in the DB (compensations and Saga transitions)
          await handleTaskFailure(workflowInstanceId, taskExecutionId, error.message, publishTaskMessage);
          
          // 7. Poison Pill Prevention: reject without requeueing
          chan.nack(msg, false, false);
          console.warn(`[RabbitMQ] Rejected message for task ${taskExecutionId} without requeuing.`);
        } catch (dbErr: any) {
          console.error(`[RabbitMQ] Failed to execute failure handler in database: ${dbErr.message}`);
          // If database itself fails, we reject with requeue=true so that when database recovers, worker can try again.
          chan.nack(msg, false, true);
        }
      }
    });

  } catch (err) {
    console.error('[RabbitMQ] Connection / subscription error:', err);
    throw err;
  }
}

export async function publishTaskMessage(
  taskExecutionId: string,
  workflowInstanceId: string,
  taskKey: string,
  inputData: any,
  worker: string
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

  const payload = {
    task_execution_id: taskExecutionId,
    workflow_instance_id: workflowInstanceId,
    task_key: taskKey,
    input_data: inputData,
  };

  const body = Buffer.from(JSON.stringify(payload));
  console.log(`[RabbitMQ] Publishing next task ${taskKey} (${taskExecutionId}) to exchange ${config.workflowExchange} using routing key ${routingKey}`);

  channel.publish(config.workflowExchange, routingKey, body, {
    persistent: true,
    contentType: 'application/json',
  });
}

export async function closeRabbitMQ() {
  console.log('[RabbitMQ] Closing channel and connection...');
  try {
    if (channel) {
      await channel.close();
    }
    if (connection) {
      await connection.close();
    }
    console.log('[RabbitMQ] Connection closed.');
  } catch (err) {
    console.error('[RabbitMQ] Error closing connection:', err);
  }
}
