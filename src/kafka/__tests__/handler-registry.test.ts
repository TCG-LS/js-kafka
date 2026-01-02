import { KafkaTopicHandlerRegistry } from '../handler-registry';
import { KafkaConnectionManager } from '../connection-manager';
import { KafkaConsumerManager } from '../consumer-manager';
import { KafkaConfig, SingleMessageHandler, BatchMessageHandler, BatchMessageHandlerWithManualCommit } from '../../interface/kafka.interface';

// Mock dependencies
jest.mock('../connection-manager');
jest.mock('../consumer-manager');
jest.mock('../../logger/logger', () => ({
    Logger: {
        getInstance: jest.fn().mockReturnValue({
            info: jest.fn(),
            warn: jest.fn(),
            error: jest.fn(),
            debug: jest.fn(),
        }),
    },
}));

const mockConsumerManager = {
    setSingleTopicHandler: jest.fn(),
    setBatchTopicHandler: jest.fn(),
    setBatchTopicHandlerWithManualCommit: jest.fn(),
} as any;

const mockConnection = {} as any;

// Mock KafkaConsumerManager.getInstance
(KafkaConsumerManager.getInstance as jest.Mock) = jest.fn().mockReturnValue(mockConsumerManager);

describe('KafkaTopicHandlerRegistry', () => {
    const mockConfig: KafkaConfig = {
        env: 'test',
        brokers: ['localhost:9092'],
        clientId: 'test-client',
        serviceName: 'test-service',
        consumerGroupId: 'test-group',
    };

    let registry: KafkaTopicHandlerRegistry;

    beforeEach(() => {
        jest.clearAllMocks();
        registry = new KafkaTopicHandlerRegistry(mockConnection, mockConfig);
    });

    describe('Constructor', () => {
        it('should initialize with connection and config', () => {
            expect(KafkaConsumerManager.getInstance).toHaveBeenCalledWith(
                mockConnection,
                mockConfig
            );
        });
    });

    describe('registerSingle', () => {
        it('should register single message handler without options', () => {
            const topic = 'user-created';
            const handler: SingleMessageHandler = jest.fn();

            registry.registerSingle(topic, handler);

            expect(mockConsumerManager.setSingleTopicHandler).toHaveBeenCalledWith(
                topic,
                handler,
                undefined
            );
        });

        it('should register single message handler with options', () => {
            const topic = 'user-created';
            const handler: SingleMessageHandler = jest.fn();
            const options = {
                consumerGroup: 'custom-group',
                fromBeginning: true,
                maxBytes: 2048,
                sessionTimeout: 30000,
                heartbeatInterval: 15000,
            };

            registry.registerSingle(topic, handler, options);

            expect(mockConsumerManager.setSingleTopicHandler).toHaveBeenCalledWith(
                topic,
                handler,
                options
            );
        });

        it('should handle multiple single handler registrations', () => {
            const handler1: SingleMessageHandler = jest.fn();
            const handler2: SingleMessageHandler = jest.fn();
            const handler3: SingleMessageHandler = jest.fn();

            registry.registerSingle('topic1', handler1);
            registry.registerSingle('topic2', handler2);
            registry.registerSingle('topic3', handler3);

            expect(mockConsumerManager.setSingleTopicHandler).toHaveBeenCalledTimes(3);
            expect(mockConsumerManager.setSingleTopicHandler).toHaveBeenNthCalledWith(
                1, 'topic1', handler1, undefined
            );
            expect(mockConsumerManager.setSingleTopicHandler).toHaveBeenNthCalledWith(
                2, 'topic2', handler2, undefined
            );
            expect(mockConsumerManager.setSingleTopicHandler).toHaveBeenNthCalledWith(
                3, 'topic3', handler3, undefined
            );
        });

        it('should register handler for same topic multiple times', () => {
            const topic = 'user-events';
            const handler1: SingleMessageHandler = jest.fn();
            const handler2: SingleMessageHandler = jest.fn();

            registry.registerSingle(topic, handler1);
            registry.registerSingle(topic, handler2);

            expect(mockConsumerManager.setSingleTopicHandler).toHaveBeenCalledTimes(2);
            expect(mockConsumerManager.setSingleTopicHandler).toHaveBeenNthCalledWith(
                1, topic, handler1, undefined
            );
            expect(mockConsumerManager.setSingleTopicHandler).toHaveBeenNthCalledWith(
                2, topic, handler2, undefined
            );
        });
    });

    describe('registerBatch', () => {
        it('should register batch message handler without options', () => {
            const topic = 'user-batch-events';
            const handler: BatchMessageHandler = jest.fn();

            registry.registerBatch(topic, handler);

            expect(mockConsumerManager.setBatchTopicHandler).toHaveBeenCalledWith(
                topic,
                handler,
                undefined
            );
        });

        it('should register batch message handler with options', () => {
            const topic = 'user-batch-events';
            const handler: BatchMessageHandler = jest.fn();
            const options = {
                consumerGroup: 'batch-group',
                fromBeginning: false,
                maxBytes: 4096,
                sessionTimeout: 60000,
                heartbeatInterval: 30000,
            };

            registry.registerBatch(topic, handler, options);

            expect(mockConsumerManager.setBatchTopicHandler).toHaveBeenCalledWith(
                topic,
                handler,
                options
            );
        });

        it('should handle multiple batch handler registrations', () => {
            const handler1: BatchMessageHandler = jest.fn();
            const handler2: BatchMessageHandler = jest.fn();
            const handler3: BatchMessageHandler = jest.fn();

            registry.registerBatch('batch-topic1', handler1);
            registry.registerBatch('batch-topic2', handler2);
            registry.registerBatch('batch-topic3', handler3);

            expect(mockConsumerManager.setBatchTopicHandler).toHaveBeenCalledTimes(3);
            expect(mockConsumerManager.setBatchTopicHandler).toHaveBeenNthCalledWith(
                1, 'batch-topic1', handler1, undefined
            );
            expect(mockConsumerManager.setBatchTopicHandler).toHaveBeenNthCalledWith(
                2, 'batch-topic2', handler2, undefined
            );
            expect(mockConsumerManager.setBatchTopicHandler).toHaveBeenNthCalledWith(
                3, 'batch-topic3', handler3, undefined
            );
        });

        it('should register handler for same topic multiple times', () => {
            const topic = 'batch-events';
            const handler1: BatchMessageHandler = jest.fn();
            const handler2: BatchMessageHandler = jest.fn();

            registry.registerBatch(topic, handler1);
            registry.registerBatch(topic, handler2);

            expect(mockConsumerManager.setBatchTopicHandler).toHaveBeenCalledTimes(2);
            expect(mockConsumerManager.setBatchTopicHandler).toHaveBeenNthCalledWith(
                1, topic, handler1, undefined
            );
            expect(mockConsumerManager.setBatchTopicHandler).toHaveBeenNthCalledWith(
                2, topic, handler2, undefined
            );
        });
    });

    describe('registerBatchWithManualCommit', () => {
        it('should register batch message handler with manual commit without options', () => {
            const topic = 'user-manual-batch-events';
            const handler: BatchMessageHandlerWithManualCommit = jest.fn();

            registry.registerBatchWithManualCommit(topic, handler);

            expect(mockConsumerManager.setBatchTopicHandlerWithManualCommit).toHaveBeenCalledWith(
                topic,
                handler,
                undefined
            );
        });

        it('should register batch message handler with manual commit with options', () => {
            const topic = 'user-manual-batch-events';
            const handler: BatchMessageHandlerWithManualCommit = jest.fn();
            const options = {
                consumerGroup: 'manual-commit-group',
                fromBeginning: false,
                maxBytes: 4096,
                sessionTimeout: 60000,
                heartbeatInterval: 30000,
            };

            registry.registerBatchWithManualCommit(topic, handler, options);

            expect(mockConsumerManager.setBatchTopicHandlerWithManualCommit).toHaveBeenCalledWith(
                topic,
                handler,
                options
            );
        });

        it('should handle multiple manual commit handler registrations', () => {
            const handler1: BatchMessageHandlerWithManualCommit = jest.fn();
            const handler2: BatchMessageHandlerWithManualCommit = jest.fn();
            const handler3: BatchMessageHandlerWithManualCommit = jest.fn();

            registry.registerBatchWithManualCommit('manual-topic1', handler1);
            registry.registerBatchWithManualCommit('manual-topic2', handler2);
            registry.registerBatchWithManualCommit('manual-topic3', handler3);

            expect(mockConsumerManager.setBatchTopicHandlerWithManualCommit).toHaveBeenCalledTimes(3);
            expect(mockConsumerManager.setBatchTopicHandlerWithManualCommit).toHaveBeenNthCalledWith(
                1, 'manual-topic1', handler1, undefined
            );
            expect(mockConsumerManager.setBatchTopicHandlerWithManualCommit).toHaveBeenNthCalledWith(
                2, 'manual-topic2', handler2, undefined
            );
            expect(mockConsumerManager.setBatchTopicHandlerWithManualCommit).toHaveBeenNthCalledWith(
                3, 'manual-topic3', handler3, undefined
            );
        });

        it('should register handler for same topic multiple times', () => {
            const topic = 'manual-batch-events';
            const handler1: BatchMessageHandlerWithManualCommit = jest.fn();
            const handler2: BatchMessageHandlerWithManualCommit = jest.fn();

            registry.registerBatchWithManualCommit(topic, handler1);
            registry.registerBatchWithManualCommit(topic, handler2);

            expect(mockConsumerManager.setBatchTopicHandlerWithManualCommit).toHaveBeenCalledTimes(2);
            expect(mockConsumerManager.setBatchTopicHandlerWithManualCommit).toHaveBeenNthCalledWith(
                1, topic, handler1, undefined
            );
            expect(mockConsumerManager.setBatchTopicHandlerWithManualCommit).toHaveBeenNthCalledWith(
                2, topic, handler2, undefined
            );
        });

        it('should accept async manual commit handlers', () => {
            const asyncManualCommitHandler: BatchMessageHandlerWithManualCommit = async (params) => {
                // Async manual commit handler implementation
                const { resolveOffset, offset } = params;
                await Promise.resolve();
                resolveOffset(offset!);
            };

            registry.registerBatchWithManualCommit('async-manual-topic', asyncManualCommitHandler);

            expect(mockConsumerManager.setBatchTopicHandlerWithManualCommit).toHaveBeenCalledWith(
                'async-manual-topic',
                asyncManualCommitHandler,
                undefined
            );
        });
    });

    describe('Mixed Handler Registration', () => {
        it('should handle single, batch, and manual commit handler registrations', () => {
            const singleHandler: SingleMessageHandler = jest.fn();
            const batchHandler: BatchMessageHandler = jest.fn();
            const manualCommitHandler: BatchMessageHandlerWithManualCommit = jest.fn();

            registry.registerSingle('single-topic', singleHandler);
            registry.registerBatch('batch-topic', batchHandler);
            registry.registerBatchWithManualCommit('manual-topic', manualCommitHandler);

            expect(mockConsumerManager.setSingleTopicHandler).toHaveBeenCalledWith(
                'single-topic',
                singleHandler,
                undefined
            );
            expect(mockConsumerManager.setBatchTopicHandler).toHaveBeenCalledWith(
                'batch-topic',
                batchHandler,
                undefined
            );
            expect(mockConsumerManager.setBatchTopicHandlerWithManualCommit).toHaveBeenCalledWith(
                'manual-topic',
                manualCommitHandler,
                undefined
            );
        });

        it('should handle same topic with different handler types', () => {
            const topic = 'mixed-topic';
            const singleHandler: SingleMessageHandler = jest.fn();
            const batchHandler: BatchMessageHandler = jest.fn();
            const manualCommitHandler: BatchMessageHandlerWithManualCommit = jest.fn();

            registry.registerSingle(topic, singleHandler);
            registry.registerBatch(topic, batchHandler);
            registry.registerBatchWithManualCommit(topic, manualCommitHandler);

            expect(mockConsumerManager.setSingleTopicHandler).toHaveBeenCalledWith(
                topic,
                singleHandler,
                undefined
            );
            expect(mockConsumerManager.setBatchTopicHandler).toHaveBeenCalledWith(
                topic,
                batchHandler,
                undefined
            );
            expect(mockConsumerManager.setBatchTopicHandlerWithManualCommit).toHaveBeenCalledWith(
                topic,
                manualCommitHandler,
                undefined
            );
        });
    });

    describe('Handler Function Types', () => {
        it('should accept async single message handlers', () => {
            const asyncHandler: SingleMessageHandler = async (params) => {
                // Async handler implementation
                await Promise.resolve();
            };

            registry.registerSingle('async-topic', asyncHandler);

            expect(mockConsumerManager.setSingleTopicHandler).toHaveBeenCalledWith(
                'async-topic',
                asyncHandler,
                undefined
            );
        });

        it('should accept async batch message handlers', () => {
            const asyncBatchHandler: BatchMessageHandler = async (params) => {
                // Async batch handler implementation
                await Promise.resolve();
            };

            registry.registerBatch('async-batch-topic', asyncBatchHandler);

            expect(mockConsumerManager.setBatchTopicHandler).toHaveBeenCalledWith(
                'async-batch-topic',
                asyncBatchHandler,
                undefined
            );
        });

        it('should accept async manual commit batch handlers', () => {
            const asyncManualCommitHandler: BatchMessageHandlerWithManualCommit = async (params) => {
                // Async manual commit handler implementation
                const { resolveOffset, offset, messages } = params;
                
                // Process messages
                for (const message of messages) {
                    await Promise.resolve(); // Simulate async processing
                }
                
                // Manual commit
                resolveOffset(offset!);
            };

            registry.registerBatchWithManualCommit('async-manual-batch-topic', asyncManualCommitHandler);

            expect(mockConsumerManager.setBatchTopicHandlerWithManualCommit).toHaveBeenCalledWith(
                'async-manual-batch-topic',
                asyncManualCommitHandler,
                undefined
            );
        });
    });

    describe('Options Validation', () => {
        it('should pass through all option properties for single handlers', () => {
            const topic = 'options-topic';
            const handler: SingleMessageHandler = jest.fn();
            const options = {
                consumerGroup: 'test-group',
                fromBeginning: true,
                maxBytes: 1024,
                sessionTimeout: 45000,
                heartbeatInterval: 20000,
            };

            registry.registerSingle(topic, handler, options);

            expect(mockConsumerManager.setSingleTopicHandler).toHaveBeenCalledWith(
                topic,
                handler,
                options
            );
        });

        it('should pass through all option properties for batch handlers', () => {
            const topic = 'batch-options-topic';
            const handler: BatchMessageHandler = jest.fn();
            const options = {
                consumerGroup: 'batch-test-group',
                fromBeginning: false,
                maxBytes: 8192,
                sessionTimeout: 90000,
                heartbeatInterval: 45000,
            };

            registry.registerBatch(topic, handler, options);

            expect(mockConsumerManager.setBatchTopicHandler).toHaveBeenCalledWith(
                topic,
                handler,
                options
            );
        });

        it('should pass through all option properties for manual commit handlers', () => {
            const topic = 'manual-commit-options-topic';
            const handler: BatchMessageHandlerWithManualCommit = jest.fn();
            const options = {
                consumerGroup: 'manual-commit-test-group',
                fromBeginning: true,
                maxBytes: 16384,
                sessionTimeout: 120000,
                heartbeatInterval: 60000,
            };

            registry.registerBatchWithManualCommit(topic, handler, options);

            expect(mockConsumerManager.setBatchTopicHandlerWithManualCommit).toHaveBeenCalledWith(
                topic,
                handler,
                options
            );
        });

        it('should handle partial options', () => {
            const topic = 'partial-options-topic';
            const handler: SingleMessageHandler = jest.fn();
            const partialOptions = {
                consumerGroup: 'partial-group',
                fromBeginning: true,
            };

            registry.registerSingle(topic, handler, partialOptions);

            expect(mockConsumerManager.setSingleTopicHandler).toHaveBeenCalledWith(
                topic,
                handler,
                partialOptions
            );
        });
    });
});