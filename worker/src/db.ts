import { Pool } from 'pg';
import { config } from './config';

export const pool = new Pool({
  connectionString: config.databaseUrl,
});

pool.on('error', (err) => {
  console.error('[Database] Unexpected error on idle client', err);
});

export interface TaskDefinition {
  worker: string;
  input_data: any;
  dependencies: string[];
  compensation?: string;
}

export interface DAG {
  tasks: {
    [key: string]: TaskDefinition;
  };
}

export async function checkTaskStatus(taskExecutionId: string): Promise<string | null> {
  const query = 'SELECT status FROM task_executions WHERE id = $1';
  const res = await pool.query(query, [taskExecutionId]);
  if (res.rows.length === 0) {
    return null;
  }
  return res.rows[0].status;
}

export async function updateTaskStatus(
  taskExecutionId: string,
  status: string,
  outputData?: any,
  errorMessage?: string
): Promise<void> {
  const fields = ['status = $2', 'updated_at = NOW()'];
  const values: any[] = [taskExecutionId, status];

  let paramIndex = 3;
  if (outputData !== undefined) {
    fields.push(`output_data = $${paramIndex++}`);
    values.push(JSON.stringify(outputData));
  }
  if (errorMessage !== undefined) {
    fields.push(`error_message = $${paramIndex++}`);
    values.push(errorMessage);
  }

  const query = `
    UPDATE task_executions 
    SET ${fields.join(', ')} 
    WHERE id = $1
  `;
  await pool.query(query, values);
  console.log(`[Database] Task execution ${taskExecutionId} status updated to ${status}`);
}

export async function completeTaskAndProgress(
  taskExecutionId: string,
  publishCallback: (taskExecId: string, workflowId: string, taskKey: string, inputData: any, worker: string) => Promise<void>
): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // 1. Mark task as COMPLETED
    const updateQuery = `
      UPDATE task_executions
      SET status = 'COMPLETED', output_data = '{"status":"success"}', updated_at = NOW()
      WHERE id = $1
      RETURNING workflow_instance_id
    `;
    const updateRes = await client.query(updateQuery, [taskExecutionId]);
    if (updateRes.rows.length === 0) {
      throw new Error(`Task execution ${taskExecutionId} not found`);
    }
    const workflowId = updateRes.rows[0].workflow_instance_id;

    await client.query('COMMIT');
    console.log(`[Database] Task ${taskExecutionId} marked COMPLETED inside transaction.`);

    // 2. Progress DAG execution
    await progressDAG(workflowId, publishCallback);

  } catch (err) {
    await client.query('ROLLBACK');
    console.error(`[Database] Failed to complete task and progress DAG: ${err}`);
    throw err;
  } finally {
    client.release();
  }
}

export async function progressDAG(
  workflowId: string,
  publishCallback: (taskExecId: string, workflowId: string, taskKey: string, inputData: any, worker: string) => Promise<void>
): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // Fetch workflow instance
    const wfRes = await client.query('SELECT status, dag FROM workflow_instances WHERE id = $1 FOR UPDATE', [workflowId]);
    if (wfRes.rows.length === 0) {
      throw new Error(`Workflow instance ${workflowId} not found`);
    }
    const workflow = wfRes.rows[0];
    const dag: DAG = workflow.dag;
    const workflowStatus = workflow.status;

    // Fetch all task executions for this workflow
    const execsRes = await client.query(
      'SELECT id, task_key, status FROM task_executions WHERE workflow_instance_id = $1',
      [workflowId]
    );
    const executions = execsRes.rows;

    const execMap = new Map<string, { id: string; status: string }>();
    for (const exec of executions) {
      execMap.set(exec.task_key, { id: exec.id, status: exec.status });
    }

    if (workflowStatus === 'COMPENSATING') {
      // Check if all active compensation tasks are finished
      let compensationsActive = false;
      const compensationTaskKeys = new Set<string>();

      for (const taskKey of Object.keys(dag.tasks)) {
        const taskDef = dag.tasks[taskKey];
        if (taskDef.compensation) {
          compensationTaskKeys.add(taskDef.compensation);
        }
      }

      for (const compKey of compensationTaskKeys) {
        const compExec = execMap.get(compKey);
        if (compExec) {
          if (compExec.status === 'RUNNING' || compExec.status === 'PENDING') {
            compensationsActive = true;
            break;
          }
        }
      }

      if (!compensationsActive) {
        console.log(`[Database] All compensation tasks completed. Transitioning workflow ${workflowId} to FAILED.`);
        await client.query('UPDATE workflow_instances SET status = $1, updated_at = NOW() WHERE id = $2', ['FAILED', workflowId]);
      }

      await client.query('COMMIT');
      return;
    }

    // Standard execution mode (RUNNING/PENDING)
    let anyTaskRunning = false;
    let anyTaskPending = false;
    const tasksToTrigger: { id: string; taskKey: string; inputData: any; worker: string }[] = [];

    for (const taskKey of Object.keys(dag.tasks)) {
      const taskDef = dag.tasks[taskKey];
      const exec = execMap.get(taskKey);

      if (!exec) continue;

      if (exec.status === 'RUNNING') {
        anyTaskRunning = true;
      } else if (exec.status === 'PENDING') {
        anyTaskPending = true;

        // Evaluate dependencies
        let dependenciesMet = true;
        const dependencies = taskDef.dependencies || [];

        for (const depKey of dependencies) {
          if (depKey === '__SAGA_FAIL__') {
            dependenciesMet = false;
            break;
          }
          const depExec = execMap.get(depKey);
          if (!depExec || depExec.status !== 'COMPLETED') {
            dependenciesMet = false;
            break;
          }
        }

        if (dependenciesMet) {
          tasksToTrigger.push({
            id: exec.id,
            taskKey: taskKey,
            inputData: taskDef.input_data,
            worker: taskDef.worker,
          });
        }
      }
    }

    // Trigger ready tasks
    for (const task of tasksToTrigger) {
      await client.query(
        "UPDATE task_executions SET status = 'RUNNING', updated_at = NOW() WHERE id = $1",
        [task.id]
      );
      // Publish task message to broker
      await publishCallback(task.id, workflowId, task.taskKey, task.inputData, task.worker);
      anyTaskRunning = true;
    }

    // If no tasks are running and none are pending, the entire workflow is successfully complete!
    if (!anyTaskRunning && !anyTaskPending) {
      console.log(`[Database] All DAG tasks completed successfully! Transitioning workflow ${workflowId} status to COMPLETED.`);
      await client.query('UPDATE workflow_instances SET status = $1, updated_at = NOW() WHERE id = $2', ['COMPLETED', workflowId]);
    }

    await client.query('COMMIT');

  } catch (err) {
    await client.query('ROLLBACK');
    console.error(`[Database] Error processing DAG state for workflow ${workflowId}:`, err);
    throw err;
  } finally {
    client.release();
  }
}

export async function handleTaskFailure(
  workflowId: string,
  failedTaskExecId: string,
  errorMessage: string,
  publishCallback: (taskExecId: string, workflowId: string, taskKey: string, inputData: any, worker: string) => Promise<void>
): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    console.warn(`[Database] Handling failure of task ${failedTaskExecId}. Transitioning workflow ${workflowId} to COMPENSATING.`);

    // 1. Mark the failed task execution status as FAILED
    await client.query(
      "UPDATE task_executions SET status = 'FAILED', error_message = $1, updated_at = NOW() WHERE id = $2",
      [errorMessage, failedTaskExecId]
    );

    // 2. Transition Workflow Instance to COMPENSATING
    await client.query(
      "UPDATE workflow_instances SET status = 'COMPENSATING', updated_at = NOW() WHERE id = $1",
      [workflowId]
    );

    // Fetch all task executions for this workflow
    const execsRes = await client.query(
      'SELECT id, task_key, status FROM task_executions WHERE workflow_instance_id = $1',
      [workflowId]
    );
    const executions = execsRes.rows;

    const execMap = new Map<string, { id: string; status: string }>();
    for (const exec of executions) {
      execMap.set(exec.task_key, { id: exec.id, status: exec.status });
    }

    // Fetch DAG configuration
    const wfRes = await client.query('SELECT dag FROM workflow_instances WHERE id = $1', [workflowId]);
    if (wfRes.rows.length === 0) {
      throw new Error(`Workflow instance ${workflowId} not found`);
    }
    const dag: DAG = wfRes.rows[0].dag;

    let compensationTriggered = false;

    for (const exec of executions) {
      if (exec.status === 'COMPLETED') {
        const taskDef = dag.tasks[exec.task_key];
        if (taskDef && taskDef.compensation) {
          const compensationTaskKey = taskDef.compensation;
          console.log(`[Database] Completed task ${exec.task_key} has compensating action: ${compensationTaskKey}`);

          // Find the compensating task execution record
          const compExec = execMap.get(compensationTaskKey);
          if (compExec) {
            await client.query(
              "UPDATE task_executions SET status = 'RUNNING', updated_at = NOW() WHERE id = $1",
              [compExec.id]
            );

            // Copy inputs from original task to compensating task definition
            const originalInputs = taskDef.input_data;

            // Publish compensating task message
            const compTaskDef = dag.tasks[compensationTaskKey];
            await publishCallback(compExec.id, workflowId, compensationTaskKey, originalInputs, compTaskDef.worker);
            compensationTriggered = true;
          } else {
            console.error(`[Database] Compensation task record ${compensationTaskKey} not found in database.`);
          }
        }
      }
    }

    // If no compensations were defined or triggered, fail the workflow immediately
    if (!compensationTriggered) {
      console.log(`[Database] No compensations defined for completed steps. Workflow fails immediately.`);
      await client.query(
        "UPDATE workflow_instances SET status = 'FAILED', updated_at = NOW() WHERE id = $1",
        [workflowId]
      );
    }

    await client.query('COMMIT');

  } catch (err) {
    await client.query('ROLLBACK');
    console.error(`[Database] Failed to handle task failure: ${err}`);
    throw err;
  } finally {
    client.release();
  }
}
