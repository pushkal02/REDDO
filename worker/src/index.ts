import { connectRabbitMQ, closeRabbitMQ } from './mq';
import { pool } from './db';

async function bootstrap() {
  console.log('[Worker] Starting REDDO Node.js I/O Worker microservice...');

  try {
    // Test Database connection
    console.log('[Worker] Verifying database connection pool...');
    await pool.query('SELECT 1');
    console.log('[Worker] Database connection pool verified successfully.');

    // Connect to RabbitMQ and start consuming
    await connectRabbitMQ();
    console.log('[Worker] Worker microservice is running and listening for tasks.');

  } catch (err) {
    console.error('[Worker] Critical startup failure:', err);
    process.exit(1);
  }
}

// Graceful Shutdown logic
async function shutdown(signal: string) {
  console.log(`[Worker] Received shutdown signal (${signal}). Initiating graceful shutdown...`);
  
  try {
    // 1. Close RabbitMQ consumer and connections
    await closeRabbitMQ();

    // 2. End PostgreSQL connection pool
    console.log('[Worker] Ending database connection pool...');
    await pool.end();
    console.log('[Worker] Database connection pool ended.');

    console.log('[Worker] Graceful shutdown completed. Exiting process.');
    process.exit(0);

  } catch (err) {
    console.error('[Worker] Error during graceful shutdown:', err);
    process.exit(1);
  }
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

// Start the app
bootstrap();
