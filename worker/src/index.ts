import { connectRabbitMQ, closeRabbitMQ } from './mq';
import { pool } from './db';
import { logger } from './logger';

async function bootstrap() {
  logger.info('Starting REDDO Node.js I/O Worker microservice...');

  try {
    // Test Database connection
    logger.info('Verifying database connection pool...');
    await pool.query('SELECT 1');
    logger.info('Database connection pool verified successfully.');

    // Connect to RabbitMQ and start consuming
    await connectRabbitMQ();
    logger.info('Worker microservice is running and listening for tasks.');

  } catch (err: any) {
    logger.error(`Critical startup failure: ${err.message}`);
    process.exit(1);
  }
}

// Graceful Shutdown logic
async function shutdown(signal: string) {
  logger.info(`Received shutdown signal (${signal}). Initiating graceful shutdown...`);
  
  try {
    // 1. Close RabbitMQ consumer and connections
    await closeRabbitMQ();

    // 2. End PostgreSQL connection pool
    logger.info('Ending database connection pool...');
    await pool.end();
    logger.info('Database connection pool ended.');

    logger.info('Graceful shutdown completed. Exiting process.');
    process.exit(0);

  } catch (err: any) {
    logger.error(`Error during graceful shutdown: ${err.message}`);
    process.exit(1);
  }
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

// Start the app
bootstrap();
