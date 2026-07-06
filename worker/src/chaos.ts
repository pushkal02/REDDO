export async function executeTaskWithChaos(
  taskExecutionId: string,
  taskKey: string,
  inputData: any
): Promise<any> {
  console.log(`[Chaos] Evaluating execution of task: ${taskKey} (Execution ID: ${taskExecutionId})`);
  
  // Extract potential chaos instructions
  const chaosCommand = inputData?.chaos_command || inputData?.chaos || null;
  const shouldFail = inputData?.fail === true || inputData?.fail === 'true';

  if (chaosCommand === 'ZOMBIE_HANG') {
    console.warn(`[Chaos] 🔥 TRIGGERED ZOMBIE_HANG for task ${taskKey} (${taskExecutionId}). This process thread will hang indefinitely...`);
    // Create a promise that never resolves
    await new Promise<void>(() => {
      // Intentionally empty. Event loop remains active but this execution path never finishes.
    });
    return; // Will never be reached
  }

  if (chaosCommand === 'FATAL_CRASH') {
    console.error(`[Chaos] 💀 TRIGGERED FATAL_CRASH for task ${taskKey} (${taskExecutionId}). Exiting process immediately...`);
    // Exit process immediately with non-zero exit code
    process.exit(1);
  }

  if (shouldFail) {
    console.error(`[Chaos] ⚠️ TRIGGERED DETERMINISTIC FAILURE for task ${taskKey} (${taskExecutionId}). Throwing error...`);
    throw new Error(`Deterministic failure triggered for task: ${taskKey}`);
  }

  // Normal Mock I/O Execution
  console.log(`[Chaos] Executing task ${taskKey} (Mocking I/O lag)...`);
  await new Promise((resolve) => setTimeout(resolve, 500)); // 500ms delay to simulate disk/network latency
  console.log(`[Chaos] Task ${taskKey} executed successfully.`);
  
  return {
    status: 'success',
    executed_at: new Date().toISOString(),
    processed_by: 'node-worker',
  };
}
