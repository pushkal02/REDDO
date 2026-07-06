import { logger } from './logger';

export async function executeTaskWithChaos(
  taskExecutionId: string,
  taskKey: string,
  inputData: any,
  correlationId?: string,
  requestId?: string
): Promise<any> {
  logger.info(`Evaluating execution of task: ${taskKey} (Execution ID: ${taskExecutionId})`, correlationId, requestId);
  
  // Extract potential chaos instructions
  const chaosCommand = inputData?.chaos_command || inputData?.chaos || null;
  const shouldFail = inputData?.fail === true || inputData?.fail === 'true';

  if (chaosCommand === 'ZOMBIE_HANG') {
    logger.warn(`🔥 TRIGGERED ZOMBIE_HANG for task ${taskKey} (${taskExecutionId}). This process thread will hang indefinitely...`, correlationId, requestId);
    // Create a promise that never resolves
    await new Promise<void>(() => {
      // Intentionally empty. Event loop remains active but this execution path never finishes.
    });
    return; // Will never be reached
  }

  if (chaosCommand === 'FATAL_CRASH') {
    logger.error(`💀 TRIGGERED FATAL_CRASH for task ${taskKey} (${taskExecutionId}). Exiting process immediately...`, correlationId, requestId);
    // Exit process immediately with non-zero exit code
    process.exit(1);
  }

  if (shouldFail) {
    logger.error(`⚠️ TRIGGERED DETERMINISTIC FAILURE for task ${taskKey} (${taskExecutionId}). Throwing error...`, correlationId, requestId);
    throw new Error(`Deterministic failure triggered for task: ${taskKey}`);
  }

  // Normal Mock I/O Execution
  logger.info(`Executing task ${taskKey} (Mocking I/O lag)...`, correlationId, requestId);
  await new Promise((resolve) => setTimeout(resolve, 500)); // 500ms delay to simulate disk/network latency
  logger.info(`Task ${taskKey} executed successfully.`, correlationId, requestId);
  
  return {
    status: 'success',
    executed_at: new Date().toISOString(),
    processed_by: 'node-worker',
  };
}
