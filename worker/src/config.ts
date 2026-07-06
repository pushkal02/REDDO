import dotenv from 'dotenv';
dotenv.config();

export const config = {
  databaseUrl: process.env.DATABASE_URL || 'postgres://postgres:postgres@localhost:5432/reddo?sslmode=disable',
  rabbitmqUrl: process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672/',
  nodeQueue: 'node-tasks',
  workflowExchange: 'workflow-exchange',
};
