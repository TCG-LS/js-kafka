import { KafkaProducerManager } from '../producer-manager';
import { KafkaConnectionManager } from '../connection-manager';
import { KafkaAdminManager } from '../admin-manager';
import { KafkaConfig, MessagePayload } from '../../interface/kafka.interface';
import { Utils } from '../../utils/utils';

// Mock dependencies
jest.mock('../connection-manager');
jest.mock('../admin-manager');
jest.mock('../../utils/utils');
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

const mockProducer = {
    send: jest.fn(),
};

const mockConnection = {
    createProducer: jest.fn().mockResolvedValue(mockProducer),
} as any;

const mockAdmin = {
    createTopic: jest.fn(),
} as any;

const MockedUtils = Utils as jest.Mocked<typeof Utils>;

describe('KafkaProducerManager', () => {
    const mockConfig: KafkaConfig = {
        env: 'test',
        brokers: ['localhost:9092'],
        clientId: 'test-client',
        serviceName: 'test-service',
        consumerGroupId: 'test-group',
        acks: 1,
    };

    beforeEach(() => {
        jest.clearAllMocks();
        // Reset singleton
        (KafkaProducerManager as any)._instance = null;
        MockedUtils.transformTopic.mockImplementation((topic, entityId, config) =>
            `${config.env}-${config.serviceName}-${entityId}.${topic}`
        );
    });

    describe('Singleton Pattern', () => {
        it('should return the same instance when called multiple times', () => {
            const producer1 = KafkaProducerManager.getInstance(mockConnection, mockConfig);
            const producer2 = KafkaProducerManager.getInstance(mockConnection, mockConfig);

            expect(producer1).toBe(producer2);
        });
    });

    describe('setAdmin', () => {
        it('should set the admin instance', () => {
            const producer = KafkaProducerManager.getInstance(mockConnection, mockConfig);
            producer.setAdmin(mockAdmin);

            expect((producer as any).admin).toBe(mockAdmin);
        });
    });

    describe('sendMessage', () => {
        let producer: KafkaProducerManager;

        beforeEach(() => {
            producer = KafkaProducerManager.getInstance(mockConnection, mockConfig);
            producer.setAdmin(mockAdmin);
        });

        it('should send message with basic payload', async () => {
            const topic = 'user-created';
            const message: MessagePayload = {
                value: { userId: '123', name: 'John' },
            };
            const entityId = 'org123';
            const transformedTopic = 'test-test-service-org123.user-created';

            MockedUtils.transformTopic.mockReturnValueOnce(transformedTopic);

            await producer.sendMessage(topic, message, entityId);

            expect(mockAdmin.createTopic).toHaveBeenCalledWith(transformedTopic);
            expect(mockConnection.createProducer).toHaveBeenCalledWith('default-producer');
            expect(mockProducer.send).toHaveBeenCalledWith({
                topic: transformedTopic,
                messages: [{
                    key: null,
                    value: JSON.stringify(message.value),
                    timestamp: expect.any(String),
                }],
                acks: 1,
            });
        });

        it('should send message with all payload properties', async () => {
            const topic = 'user-created';
            const message: MessagePayload = {
                key: 'user-123',
                value: { userId: '123', name: 'John' },
                timestamp: '1640995200000',
                partition: 2,
                headers: { 'content-type': 'application/json', version: '1.0' },
            };
            const entityId = 'org123';

            await producer.sendMessage(topic, message, entityId);

            expect(mockProducer.send).toHaveBeenCalledWith({
                topic: expect.any(String),
                messages: [{
                    key: 'user-123',
                    value: JSON.stringify(message.value),
                    timestamp: '1640995200000',
                    partition: 2,
                    headers: { 'content-type': 'application/json', version: '1.0' },
                }],
                acks: 1,
            });
        });

        it('should handle message without entity ID', async () => {
            const topic = 'user-created';
            const message: MessagePayload = {
                value: { userId: '123' },
            };

            await producer.sendMessage(topic, message);

            expect(Utils.transformTopic).toHaveBeenCalledWith(topic, '', mockConfig);
        });

        it('should use custom acks from options', async () => {
            const topic = 'user-created';
            const message: MessagePayload = {
                value: { userId: '123' },
            };
            const options = { acks: -1 };

            await producer.sendMessage(topic, message, '', options);

            expect(mockProducer.send).toHaveBeenCalledWith(
                expect.objectContaining({
                    acks: -1,
                })
            );
        });

        it('should use config acks when no options provided', async () => {
            const topic = 'user-created';
            const message: MessagePayload = {
                value: { userId: '123' },
            };

            await producer.sendMessage(topic, message);

            expect(mockProducer.send).toHaveBeenCalledWith(
                expect.objectContaining({
                    acks: 1,
                })
            );
        });

        it('should handle negative partition values', async () => {
            const topic = 'user-created';
            const message: MessagePayload = {
                value: { userId: '123' },
                partition: -1,
            };

            await producer.sendMessage(topic, message);

            const sentMessage = mockProducer.send.mock.calls[0][0];
            expect(sentMessage.messages[0]).not.toHaveProperty('partition');
        });

        it('should format headers correctly', async () => {
            const topic = 'user-created';
            const message: MessagePayload = {
                value: { userId: '123' },
                headers: {
                    stringHeader: 'string-value',
                    contentType: 'application/json',
                    correlationId: 'abc-123',
                    version: '2.0',
                },
            };

            await producer.sendMessage(topic, message);

            const sentMessage = mockProducer.send.mock.calls[0][0];
            expect(sentMessage.messages[0].headers).toEqual({
                stringHeader: 'string-value',
                contentType: 'application/json',
                correlationId: 'abc-123',
                version: '2.0',
            });
        });

        it('should handle producer creation failure', async () => {
            mockConnection.createProducer.mockResolvedValueOnce(null);

            const topic = 'user-created';
            const message: MessagePayload = {
                value: { userId: '123' },
            };

            await producer.sendMessage(topic, message);

            expect(mockProducer.send).not.toHaveBeenCalled();
        });

        it('should throw error when sending fails', async () => {
            const error = new Error('Send failed');
            mockProducer.send.mockRejectedValueOnce(error);

            const topic = 'user-created';
            const message: MessagePayload = {
                value: { userId: '123' },
            };

            await expect(producer.sendMessage(topic, message)).rejects.toThrow(error);
        });
    });

    describe('sendBatchMessages', () => {
        let producer: KafkaProducerManager;

        beforeEach(() => {
            producer = KafkaProducerManager.getInstance(mockConnection, mockConfig);
            producer.setAdmin(mockAdmin);
        });

        it('should send batch of messages', async () => {
            const topic = 'user-created';
            const messages: MessagePayload[] = [
                { key: 'user-1', value: { userId: '1', name: 'John' } },
                { key: 'user-2', value: { userId: '2', name: 'Jane' } },
                { key: 'user-3', value: { userId: '3', name: 'Bob' } },
            ];
            const entityId = 'org123';

            await producer.sendBatchMessages(topic, messages, entityId);

            expect(mockConnection.createProducer).toHaveBeenCalledWith('batch-producer');
            expect(mockProducer.send).toHaveBeenCalledWith({
                topic: expect.any(String),
                messages: [
                    {
                        key: 'user-1',
                        value: JSON.stringify({ userId: '1', name: 'John' }),
                        timestamp: expect.any(String),
                    },
                    {
                        key: 'user-2',
                        value: JSON.stringify({ userId: '2', name: 'Jane' }),
                        timestamp: expect.any(String),
                    },
                    {
                        key: 'user-3',
                        value: JSON.stringify({ userId: '3', name: 'Bob' }),
                        timestamp: expect.any(String),
                    },
                ],
                acks: 1,
            });
        });

        it('should handle empty message array', async () => {
            const topic = 'user-created';
            const messages: MessagePayload[] = [];

            await producer.sendBatchMessages(topic, messages);

            expect(mockProducer.send).not.toHaveBeenCalled();
        });

        it('should handle batch messages with all properties', async () => {
            const topic = 'user-created';
            const messages: MessagePayload[] = [
                {
                    key: 'user-1',
                    value: { userId: '1' },
                    timestamp: '1640995200000',
                    partition: 1,
                    headers: { type: 'create' },
                },
                {
                    key: 'user-2',
                    value: { userId: '2' },
                    partition: 2,
                    headers: { type: 'update' },
                },
            ];

            await producer.sendBatchMessages(topic, messages);

            const sentMessage = mockProducer.send.mock.calls[0][0];
            expect(sentMessage.messages).toHaveLength(2);
            expect(sentMessage.messages[0]).toMatchObject({
                key: 'user-1',
                timestamp: '1640995200000',
                partition: 1,
                headers: { type: 'create' },
            });
            expect(sentMessage.messages[1]).toMatchObject({
                key: 'user-2',
                partition: 2,
                headers: { type: 'update' },
            });
        });

        it('should use custom acks from options', async () => {
            const topic = 'user-created';
            const messages: MessagePayload[] = [
                { value: { userId: '1' } },
            ];
            const options = { acks: 0 };

            await producer.sendBatchMessages(topic, messages, '', options);

            expect(mockProducer.send).toHaveBeenCalledWith(
                expect.objectContaining({
                    acks: 0,
                })
            );
        });

        it('should handle producer creation failure', async () => {
            mockConnection.createProducer.mockResolvedValueOnce(null);

            const topic = 'user-created';
            const messages: MessagePayload[] = [
                { value: { userId: '1' } },
            ];

            await producer.sendBatchMessages(topic, messages);

            expect(mockProducer.send).not.toHaveBeenCalled();
        });

        it('should throw error when batch sending fails', async () => {
            const error = new Error('Batch send failed');
            mockProducer.send.mockRejectedValueOnce(error);

            const topic = 'user-created';
            const messages: MessagePayload[] = [
                { value: { userId: '1' } },
            ];

            await expect(producer.sendBatchMessages(topic, messages)).rejects.toThrow(error);
        });
    });

    describe('formatHeaders', () => {
        let producer: KafkaProducerManager;

        beforeEach(() => {
            producer = KafkaProducerManager.getInstance(mockConnection, mockConfig);
        });

        it('should handle string headers correctly', async () => {
            const topic = 'test-topic';
            const message: MessagePayload = {
                value: { test: 'data' },
                headers: {
                    stringValue: 'test',
                    contentType: 'application/json',
                    version: '1.0',
                    userId: '12345',
                },
            };

            producer.setAdmin(mockAdmin);
            await producer.sendMessage(topic, message);

            const sentMessage = mockProducer.send.mock.calls[0][0];
            expect(sentMessage.messages[0].headers).toEqual({
                stringValue: 'test',
                contentType: 'application/json',
                version: '1.0',
                userId: '12345',
            });
        });

        it('should handle mixed header types when passed as any', async () => {
            const topic = 'test-topic';
            // Simulate what might happen if headers come from external source with mixed types
            const rawHeaders = {
                stringValue: 'test',
                numberValue: 123,
                booleanValue: false,
                nullValue: null,
                undefinedValue: undefined,
                objectValue: { nested: 'object' },
                arrayValue: [1, 2, 3],
            };

            const message: MessagePayload = {
                value: { test: 'data' },
                headers: rawHeaders as any, // Simulate external data with mixed types
            };

            producer.setAdmin(mockAdmin);
            await producer.sendMessage(topic, message);

            const sentMessage = mockProducer.send.mock.calls[0][0];
            expect(sentMessage.messages[0].headers).toEqual({
                stringValue: 'test',
                numberValue: '123',
                booleanValue: 'false',
                nullValue: 'null',
                undefinedValue: 'undefined',
                objectValue: '[object Object]',
                arrayValue: '1,2,3',
            });
        });

        it('should preserve Buffer headers', async () => {
            const topic = 'test-topic';
            const bufferValue = Buffer.from('test buffer');
            const message: MessagePayload = {
                value: { test: 'data' },
                headers: {
                    bufferHeader: bufferValue,
                    stringHeader: 'string value',
                },
            };

            producer.setAdmin(mockAdmin);
            await producer.sendMessage(topic, message);

            const sentMessage = mockProducer.send.mock.calls[0][0];
            expect(sentMessage.messages[0].headers).toEqual({
                bufferHeader: bufferValue,
                stringHeader: 'string value',
            });
        });
    });
});