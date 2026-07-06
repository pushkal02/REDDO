export class Logger {
  private service: string;

  constructor(service: string) {
    this.service = service;
  }

  private formatMessage(level: string, correlationId: string | undefined, requestId: string | undefined, message: string): string {
    const timestamp = new Date().toISOString();
    const corr = correlationId || '-';
    const req = requestId || '-';
    return `[${timestamp}] [${level}] [${this.service}] [${corr}] [${req}] - ${message}`;
  }

  info(message: string, correlationId?: string, requestId?: string) {
    console.log(this.formatMessage('INFO', correlationId, requestId, message));
  }

  warn(message: string, correlationId?: string, requestId?: string) {
    console.warn(this.formatMessage('WARN', correlationId, requestId, message));
  }

  error(message: string, correlationId?: string, requestId?: string) {
    console.error(this.formatMessage('ERROR', correlationId, requestId, message));
  }

  debug(message: string, correlationId?: string, requestId?: string) {
    console.debug(this.formatMessage('DEBUG', correlationId, requestId, message));
  }
}

export const logger = new Logger('worker');
