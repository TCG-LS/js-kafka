/**
 * @fileoverview Simple logging utility with singleton pattern
 * @description Provides structured logging with different levels and timestamps
 */

/**
 * Simple logger implementation with singleton pattern.
 * 
 * @description Provides structured logging with timestamps and different log levels.
 * Uses console methods for output with consistent formatting across the application.
 * 
 * @example
 * ```typescript
 * const logger = Logger.getInstance();
 * logger.info('Application started');
 * logger.warn('Configuration missing, using defaults');
 * logger.error('Failed to connect to database', error);
 * ```
 */
export class Logger {
    private static _instance: Logger | null = null;

    /**
     * Private constructor to enforce singleton pattern.
     */
    constructor() { }

    /**
     * Gets the singleton instance of the Logger.
     * 
     * @returns The Logger singleton instance
     * 
     * @description Creates a new instance if none exists, otherwise returns
     * the existing instance. This ensures consistent logging throughout the application.
     */
    public static getInstance(): Logger {
        if (!Logger._instance) {
            Logger._instance = new Logger();
        }
        return Logger._instance;
    }

    /**
     * Logs informational messages.
     * 
     * @param args - Arguments to log
     * 
     * @description Outputs informational messages with [INFO] prefix and timestamp.
     * Use for general application flow information.
     * 
     * @example
     * ```typescript
     * logger.info('User logged in', { userId: '123' });
     * ```
     */
    info(...args: any[]): void {
        console.log(`[INFO]`, new Date().toISOString(), ...args);
    }

    /**
     * Logs warning messages.
     * 
     * @param args - Arguments to log
     * 
     * @description Outputs warning messages with [WARN] prefix and timestamp.
     * Use for potentially problematic situations that don't prevent operation.
     * 
     * @example
     * ```typescript
     * logger.warn('Configuration missing, using defaults');
     * ```
     */
    warn(...args: any[]): void {
        console.warn(`[WARN]`, new Date().toISOString(), ...args);
    }

    /**
     * Logs error messages.
     * 
     * @param args - Arguments to log
     * 
     * @description Outputs error messages with [ERROR] prefix and timestamp.
     * Use for error conditions that may affect application functionality.
     * 
     * @example
     * ```typescript
     * logger.error('Database connection failed', error);
     * ```
     */
    error(...args: any[]): void {
        console.error(`[ERROR]`, new Date().toISOString(), ...args);
    }

    /**
     * Logs debug messages.
     * 
     * @param args - Arguments to log
     * 
     * @description Outputs debug messages with [DEBUG] prefix and timestamp.
     * Use for detailed diagnostic information during development.
     * 
     * @example
     * ```typescript
     * logger.debug('Processing message', { messageId: 'msg-123' });
     * ```
     */
    debug(...args: any[]): void {
        console.debug(`[DEBUG]`, new Date().toISOString(), ...args);
    }
}
