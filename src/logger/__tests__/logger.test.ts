import { Logger } from '../logger';

// Mock console methods
const mockConsoleLog = jest.spyOn(console, 'log').mockImplementation();
const mockConsoleWarn = jest.spyOn(console, 'warn').mockImplementation();
const mockConsoleError = jest.spyOn(console, 'error').mockImplementation();
const mockConsoleDebug = jest.spyOn(console, 'debug').mockImplementation();

describe('Logger', () => {
    beforeEach(() => {
        jest.clearAllMocks();
        // Reset singleton instance for each test
        (Logger as any)._instance = null;
    });

    afterAll(() => {
        mockConsoleLog.mockRestore();
        mockConsoleWarn.mockRestore();
        mockConsoleError.mockRestore();
        mockConsoleDebug.mockRestore();
    });

    describe('Singleton Pattern', () => {
        it('should return the same instance when called multiple times', () => {
            const logger1 = Logger.getInstance();
            const logger2 = Logger.getInstance();

            expect(logger1).toBe(logger2);
        });

        it('should create only one instance', () => {
            const logger1 = Logger.getInstance();
            const logger2 = Logger.getInstance();
            const logger3 = Logger.getInstance();

            expect(logger1).toBe(logger2);
            expect(logger2).toBe(logger3);
        });
    });

    describe('Logging Methods', () => {
        let logger: Logger;

        beforeEach(() => {
            logger = Logger.getInstance();
        });

        it('should log info messages with timestamp', () => {
            const testMessage = 'Test info message';
            logger.info(testMessage);

            expect(mockConsoleLog).toHaveBeenCalledWith(
                '[INFO]',
                expect.any(String), // timestamp
                testMessage
            );
        });

        it('should log warn messages with timestamp', () => {
            const testMessage = 'Test warning message';
            logger.warn(testMessage);

            expect(mockConsoleWarn).toHaveBeenCalledWith(
                '[WARN]',
                expect.any(String), // timestamp
                testMessage
            );
        });

        it('should log error messages with timestamp', () => {
            const testMessage = 'Test error message';
            logger.error(testMessage);

            expect(mockConsoleError).toHaveBeenCalledWith(
                '[ERROR]',
                expect.any(String), // timestamp
                testMessage
            );
        });

        it('should log debug messages with timestamp', () => {
            const testMessage = 'Test debug message';
            logger.debug(testMessage);

            expect(mockConsoleDebug).toHaveBeenCalledWith(
                '[DEBUG]',
                expect.any(String), // timestamp
                testMessage
            );
        });

        it('should handle multiple arguments', () => {
            const arg1 = 'First argument';
            const arg2 = { key: 'value' };
            const arg3 = 123;

            logger.info(arg1, arg2, arg3);

            expect(mockConsoleLog).toHaveBeenCalledWith(
                '[INFO]',
                expect.any(String), // timestamp
                arg1,
                arg2,
                arg3
            );
        });

        it('should include valid ISO timestamp', () => {
            logger.info('test');

            const call = mockConsoleLog.mock.calls[0];
            const timestamp = call[1];

            // Check if timestamp is a valid ISO string
            expect(new Date(timestamp).toISOString()).toBe(timestamp);
        });

        it('should handle empty arguments', () => {
            logger.info();

            expect(mockConsoleLog).toHaveBeenCalledWith(
                '[INFO]',
                expect.any(String) // timestamp
            );
        });

        it('should handle null and undefined arguments', () => {
            logger.error(null, undefined, 'message');

            expect(mockConsoleError).toHaveBeenCalledWith(
                '[ERROR]',
                expect.any(String), // timestamp
                null,
                undefined,
                'message'
            );
        });
    });

    describe('Timestamp Format', () => {
        it('should use ISO string format for timestamps', () => {
            const logger = Logger.getInstance();
            const beforeTime = new Date().toISOString();
            logger.info('test');
            const afterTime = new Date().toISOString();

            expect(mockConsoleLog).toHaveBeenCalled();
            const call = mockConsoleLog.mock.calls[0];
            expect(call).toBeDefined();
            expect(call.length).toBeGreaterThan(1);
            
            if (call && call.length > 1) {
                const timestamp = call[1];
                expect(timestamp).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/);
                expect(timestamp >= beforeTime).toBe(true);
                expect(timestamp <= afterTime).toBe(true);
            }
        });
    });
});