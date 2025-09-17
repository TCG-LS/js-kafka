import { KafkaConnectionManager } from '../connection-manager';
import { KafkaConfig } from '../../interface/kafka.interface';
import { Kafka, Producer, Consumer, Admin } from 'kafkajs';

// Mock kafkajs
jest.mock('kafkajs');

const MockedKafka = Kafka as jest.MockedClass<typeof Kafka>;
const mockProducer = {
    connect: jest.fn(),
    disconnect: jest.fn(),
    send: jest.fn(),
} as any;

const mockConsumer = {
    connect: jest.fn(),
    disconnect: jest.fn(),
    subscribe: jest.fn(),
    run: jest.fn(),
    stop: jest.fn(),
} as any;

const mockAdmin = {
    connect: jest.fn(),
    disconnect: jest.fn(),
    createTopics: jest.fn(),
    listTopics: jest.fn(),
} as any;

const mockKafkaInstance = {
    producer: jest.fn().mockReturnValue(mockProducer),
    consumer: jest.fn().mockReturnValue(mockConsumer),
    admin: jest.fn().mockReturnValue(mockAdmin),
} as any;

describe('KafkaConnectionManager', () => {
    const mockConfig: KafkaConfig = {
        env: 'test',
        brokers: ['localhost:9092'],
        clientId: 'test-client',
        serviceName: 'test-service',
        consumerGroupId: 'test-group',
    };

    beforeEach(() => {
        jest.clearAllMocks();
        MockedKafka.mockImplementation(() => mockKafkaInstance);
        // Reset singleton
        (KafkaConnectionManager as any).instance = null;
    });

    describe('Singleton Pattern', () => {
        it('should return the same instance when called multiple times', () => {
            const manager1 = KafkaConnectionManager.getInstance(mockConfig);
            const manager2 = KafkaConnectionManager.getInstance(mockConfig);

            expect(manager1).toBe(manager2);
        });

        it('should create Kafka instance with correct config', () => {
            KafkaConnectionManager.getInstance(mockConfig);

            expect(MockedKafka).toHaveBeenCalledWith({
                clientId: mockConfig.clientId,
                brokers: mockConfig.brokers,
            });
        });
    });

    describe('getKafka', () => {
        it('should return the Kafka instance', () => {
            const manager = KafkaConnectionManager.getInstance(mockConfig);
            const kafka = manager.getKafka();

            expect(kafka).toBe(mockKafkaInstance);
        });
    });

    describe('createProducer', () => {
        let manager: KafkaConnectionManager;

        beforeEach(() => {
            manager = KafkaConnectionManager.getInstance(mockConfig);
        });

        it('should create and connect a new producer', async () => {
            const producerId = 'test-producer';
            
            const producer = await manager.createProducer(producerId);

            expect(mockKafkaInstance.producer).toHaveBeenCalledWith({
                allowAutoTopicCreation: false,
            });
            expect(mockProducer.connect).toHaveBeenCalled();
            expect(producer).toBe(mockProducer);
        });

        it('should return existing producer if already created', async () => {
            const producerId = 'test-producer';
            
            const producer1 = await manager.createProducer(producerId);
            const producer2 = await manager.createProducer(producerId);

            expect(producer1).toBe(producer2);
            expect(mockKafkaInstance.producer).toHaveBeenCalledTimes(1);
            expect(mockProducer.connect).toHaveBeenCalledTimes(1);
        });

        it('should handle producer creation errors', async () => {
            mockProducer.connect.mockRejectedValueOnce(new Error('Connection failed'));
            
            const producer = await manager.createProducer('test-producer');

            expect(producer).toBeUndefined();
        });
    });

    describe('createConsumer', () => {
        let manager: KafkaConnectionManager;

        beforeEach(() => {
            manager = KafkaConnectionManager.getInstance(mockConfig);
        });

        it('should create and connect a new consumer with default options', async () => {
            const groupId = 'test-group';
            
            const consumer = await manager.createConsumer(groupId);

            expect(mockKafkaInstance.consumer).toHaveBeenCalledWith({
                groupId,
                maxBytes: 262144,
                sessionTimeout: 60000,
                heartbeatInterval: 30000,
            });
            expect(mockConsumer.connect).toHaveBeenCalled();
            expect(consumer).toBe(mockConsumer);
        });

        it('should create consumer with custom options', async () => {
            const groupId = 'test-group';
            const options = {
                maxBytes: 2097152,
                sessionTimeout: 120000,
                heartbeatInterval: 60000,
            };
            
            const consumer = await manager.createConsumer(groupId, options);

            expect(mockKafkaInstance.consumer).toHaveBeenCalledWith({
                groupId,
                ...options,
            });
            expect(consumer).toBe(mockConsumer);
        });

        it('should return existing consumer if already created', async () => {
            const groupId = 'test-group';
            
            const consumer1 = await manager.createConsumer(groupId);
            const consumer2 = await manager.createConsumer(groupId);

            expect(consumer1).toBe(consumer2);
            expect(mockKafkaInstance.consumer).toHaveBeenCalledTimes(1);
            expect(mockConsumer.connect).toHaveBeenCalledTimes(1);
        });

        it('should handle consumer creation errors', async () => {
            mockConsumer.connect.mockRejectedValueOnce(new Error('Connection failed'));
            
            const consumer = await manager.createConsumer('test-group');

            expect(consumer).toBeUndefined();
        });
    });

    describe('getAdmin', () => {
        let manager: KafkaConnectionManager;

        beforeEach(() => {
            manager = KafkaConnectionManager.getInstance(mockConfig);
        });

        it('should create and connect admin client', async () => {
            const admin = await manager.getAdmin();

            expect(mockKafkaInstance.admin).toHaveBeenCalled();
            expect(mockAdmin.connect).toHaveBeenCalled();
            expect(admin).toBe(mockAdmin);
        });

        it('should return existing admin if already created', async () => {
            const admin1 = await manager.getAdmin();
            const admin2 = await manager.getAdmin();

            expect(admin1).toBe(admin2);
            expect(mockKafkaInstance.admin).toHaveBeenCalledTimes(1);
            expect(mockAdmin.connect).toHaveBeenCalledTimes(1);
        });
    });

    describe('shutdown', () => {
        let manager: KafkaConnectionManager;

        beforeEach(() => {
            manager = KafkaConnectionManager.getInstance(mockConfig);
        });

        it('should disconnect all producers, consumers, and admin', async () => {
            // Create some producers and consumers
            await manager.createProducer('producer1');
            await manager.createProducer('producer2');
            await manager.createConsumer('consumer1');
            await manager.createConsumer('consumer2');
            await manager.getAdmin();

            await manager.shutdown();

            expect(mockProducer.disconnect).toHaveBeenCalledTimes(2);
            expect(mockConsumer.disconnect).toHaveBeenCalledTimes(2);
            expect(mockAdmin.disconnect).toHaveBeenCalledTimes(1);
        });

        it('should handle shutdown errors gracefully', async () => {
            await manager.createProducer('producer1');
            mockProducer.disconnect.mockRejectedValueOnce(new Error('Disconnect failed'));

            await expect(manager.shutdown()).resolves.not.toThrow();
        });

        it('should clear all connections after shutdown', async () => {
            await manager.createProducer('producer1');
            await manager.createConsumer('consumer1');
            await manager.getAdmin();

            await manager.shutdown();

            // Try to get admin again - should create new one
            const admin = await manager.getAdmin();
            expect(mockKafkaInstance.admin).toHaveBeenCalledTimes(2);
        });

        it('should handle shutdown when no connections exist', async () => {
            await expect(manager.shutdown()).resolves.not.toThrow();
        });
    });
});